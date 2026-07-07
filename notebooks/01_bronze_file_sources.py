# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — File-Based Sources
# MAGIC ### `employees.xlsx` · `departments.csv` · `job_roles.csv` · `employee_events.csv`
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Reads 4 flat files from the ADLS landing zone and writes each one to a
# MAGIC Delta table in the bronze layer — with zero transformations.
# MAGIC
# MAGIC **Bronze principle (important — understand this before Silver):**
# MAGIC Bronze = raw source data, preserved exactly as received.
# MAGIC We do NOT clean, cast, rename, or filter here.
# MAGIC The only things bronze does are:
# MAGIC   1. Read the raw file
# MAGIC   2. Add audit metadata columns (`_source_file`, `_ingested_at`)
# MAGIC   3. Write to Delta format in ADLS
# MAGIC
# MAGIC Why? Because if Silver logic ever has a bug, you can re-run Silver
# MAGIC against the original bronze data without needing to re-fetch the source.
# MAGIC Bronze is your source of truth and your safety net — never transform it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config
# MAGIC
# MAGIC All paths in one place. If your mount point ever changes, you update it here
# MAGIC and every cell below picks it up automatically — no hunting through 20 notebooks.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, BooleanType, DoubleType
)

# Root paths — update MOUNT_ROOT if your mount path ever changes
MOUNT_ROOT    = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
LANDING_PATH  = f"{MOUNT_ROOT}/bronze/landing"
BRONZE_PATH   = f"{MOUNT_ROOT}/bronze"

print(f"Landing zone : {LANDING_PATH}")
print(f"Bronze output: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify landing zone files are present
# MAGIC
# MAGIC Always check the files exist before reading them.
# MAGIC If a file is missing, you want a clear error NOW — not a confusing
# MAGIC "table is empty" problem 3 notebooks later in Silver.

# COMMAND ----------

landing_files = [f.name for f in dbutils.fs.ls(LANDING_PATH)]
print("Files in landing zone:")
for f in landing_files:
    print(f"  {f}")

# Assert the files we need actually exist — fail fast if not
required_files = [
    "employees.xlsx",
    "departments.csv",
    "job_roles.csv",
    "employee_events.csv"
]
missing = [f for f in required_files if f not in landing_files]
if missing:
    raise FileNotFoundError(f"Missing files in landing zone: {missing}")

print("\nAll required files present. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper: add audit columns
# MAGIC
# MAGIC Every bronze table gets two extra columns added at write time:
# MAGIC - `_source_file`  : which file this row came from (critical for debugging)
# MAGIC - `_ingested_at`  : when this notebook ran (for lineage tracking)
# MAGIC
# MAGIC WHY UNDERSCORE PREFIX:
# MAGIC Prefixing with `_` is a convention that signals "this column was added by
# MAGIC the pipeline, not from the source." It keeps audit columns visually distinct
# MAGIC from business columns. Your Silver notebook will drop these after use.

# COMMAND ----------

def add_audit_columns(df, source_filename: str):
    """
    Adds _source_file and _ingested_at to any DataFrame.
    Called identically for every source — consistent audit trail across all tables.
    """
    return (
        df
        .withColumn("_source_file", F.lit(source_filename))
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. departments.csv → bronze.departments
# MAGIC
# MAGIC Starting with the simplest file (4 rows, no complexity) to establish
# MAGIC the pattern cleanly before we get to employees.
# MAGIC
# MAGIC **Why explicit schema instead of inferSchema=True?**
# MAGIC `inferSchema=True` reads the file twice (one pass to infer, one to read)
# MAGIC and can silently get types wrong on small files (e.g. inferring "DPT01"
# MAGIC as StringType is fine, but a column with only integers in the first 100 rows
# MAGIC might be inferred as IntegerType even if it can contain strings later).
# MAGIC Explicit schema = no surprises, documented intent, single read pass.

# COMMAND ----------

departments_schema = StructType([
    StructField("department_id",   StringType(), nullable=False),
    StructField("department_name", StringType(), nullable=False),
    StructField("cost_center",     StringType(), nullable=True),
])

df_departments_raw = (
    spark.read
    .option("header", "true")
    .schema(departments_schema)
    .csv(f"{LANDING_PATH}/departments.csv")
)

df_departments = add_audit_columns(df_departments_raw, "departments.csv")

print(f"departments row count: {df_departments.count()}")
df_departments.printSchema()
df_departments.show(truncate=False)

# COMMAND ----------

# Write to Delta — mode("overwrite") makes this notebook idempotent:
# running it twice produces the same result as running it once.
# This matters because ADF may retry a failed run — you don't want duplicate rows.

(
    df_departments
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/departments")
)

print("Written: bronze/departments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. job_roles.csv → bronze.job_roles

# COMMAND ----------

job_roles_schema = StructType([
    StructField("job_role_id", StringType(), nullable=False),
    StructField("job_title",   StringType(), nullable=False),
    StructField("job_level",   StringType(), nullable=False),
])

df_job_roles_raw = (
    spark.read
    .option("header", "true")
    .schema(job_roles_schema)
    .csv(f"{LANDING_PATH}/job_roles.csv")
)

df_job_roles = add_audit_columns(df_job_roles_raw, "job_roles.csv")

print(f"job_roles row count: {df_job_roles.count()}")
df_job_roles.show(truncate=False)

(
    df_job_roles
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/job_roles")
)

print("Written: bronze/job_roles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. employee_events.csv → bronze.employee_events
# MAGIC
# MAGIC This is the most important file in the whole pipeline.
# MAGIC Every SCD2 version in dim_employee and every row in both fact tables
# MAGIC is derived from this event log — so we treat it with extra care:
# MAGIC   - Explicit schema (no type drift allowed)
# MAGIC   - Row count assertion after write to catch silent empty-file scenarios
# MAGIC   - event_date kept as StringType in bronze (Silver will cast to DateType)
# MAGIC
# MAGIC WHY event_date AS STRING IN BRONZE:
# MAGIC Bronze preserves the source exactly. If the source CSV has a bad date
# MAGIC like "2026-13-01" (month 13), we want that bad value to land in bronze
# MAGIC and be caught + handled explicitly in Silver — not silently nulled out
# MAGIC by a DateType cast at read time with no error raised.

# COMMAND ----------

employee_events_schema = StructType([
    StructField("event_id",             StringType(),  nullable=False),
    StructField("employee_id",          StringType(),  nullable=False),
    StructField("event_type",           StringType(),  nullable=False),
    StructField("event_date",           StringType(),  nullable=False),  # cast in Silver
    StructField("department_id",        StringType(),  nullable=True),
    StructField("job_role_id",          StringType(),  nullable=True),
    StructField("manager_employee_id",  StringType(),  nullable=True),
    StructField("salary",               IntegerType(), nullable=True),
    StructField("notes",                StringType(),  nullable=True),
])

df_events_raw = (
    spark.read
    .option("header", "true")
    .schema(employee_events_schema)
    .csv(f"{LANDING_PATH}/employee_events.csv")
)

df_events = add_audit_columns(df_events_raw, "employee_events.csv")

row_count = df_events.count()
print(f"employee_events row count: {row_count}")

# Fail fast if event log is empty — this would silently break all downstream tables
if row_count == 0:
    raise ValueError("employee_events is empty — aborting bronze write.")

df_events.printSchema()
df_events.show(10, truncate=False)

# COMMAND ----------

(
    df_events
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/employee_events")
)

print(f"Written: bronze/employee_events ({row_count} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. employees.xlsx → bronze.employees
# MAGIC
# MAGIC Excel is the trickiest source here because PySpark has no native Excel reader.
# MAGIC We use the `com.crealytics.spark.excel` library, which must be installed on
# MAGIC your cluster. If you get a ClassNotFoundException, go to:
# MAGIC   Cluster → Libraries → Install New → Maven
# MAGIC   Coordinates: com.crealytics:spark-excel_2.12:3.3.1_0.18.7
# MAGIC
# MAGIC WHY NOT CONVERT TO CSV FIRST:
# MAGIC You could pre-convert Excel to CSV, but in real pipelines your HRBP will
# MAGIC always send you .xlsx files. Learning to read Excel directly is more
# MAGIC representative of what you'll actually face.
# MAGIC
# MAGIC IMPORTANT: All columns read as StringType from Excel at bronze layer.
# MAGIC Excel mixes types inside cells in ways that cause type inference to fail
# MAGIC silently. StringType is safe — Silver will cast everything explicitly.

# COMMAND ----------

df_employees_raw = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "false")       # always false for Excel at bronze
    .option("dataAddress", "'employees'!A1")  # sheet name + start cell
    .load(f"{LANDING_PATH}/employees.xlsx")
)

df_employees = add_audit_columns(df_employees_raw, "employees.xlsx")

row_count = df_employees.count()
print(f"employees row count: {row_count}")

if row_count == 0:
    raise ValueError("employees.xlsx returned 0 rows — check sheet name and dataAddress option.")

df_employees.printSchema()
df_employees.show(truncate=False)

# COMMAND ----------

# Spot-check: verify the trailing whitespace emails are preserved in bronze
# (they should be -- we're not cleaning here, just proving they landed as-is)
print("Email column sample (check for trailing whitespace in raw values):")
df_employees.select("employee_id", "email").show(20, truncate=False)

# COMMAND ----------

(
    df_employees
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/employees")
)

print(f"Written: bronze/employees ({row_count} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify all 4 bronze tables written successfully

# COMMAND ----------

bronze_tables = ["employees", "departments", "job_roles", "employee_events"]

print("Bronze layer verification:")
print("-" * 45)
for table in bronze_tables:
    path = f"{BRONZE_PATH}/{table}"
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"  {table:<22} {count:>4} rows  OK")
    except Exception as e:
        print(f"  {table:<22} FAILED: {e}")

print("-" * 45)
print("Bronze Notebook 1 complete.")
print("Next: run 02_bronze_api_sources to ingest JSON sources.")
