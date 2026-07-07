# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Employee Events
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Cleans, casts, and validates `bronze.employee_events` into a trusted
# MAGIC `silver.employee_events` table ready for Gold fact table derivation.
# MAGIC
# MAGIC **Why employee_events gets its own silver notebook:**
# MAGIC The events table is the source for BOTH fact tables in Gold:
# MAGIC   - `fact_employee_events` reads directly from silver.employee_events
# MAGIC   - `fact_headcount_snapshot` is derived by aggregating silver.employee_events
# MAGIC     month by month
# MAGIC
# MAGIC Getting the types and validation right here protects both downstream tables.
# MAGIC A bad event_date or an unrecognised event_type here would silently corrupt
# MAGIC both Gold facts — so we validate aggressively.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import count as spark_count

MOUNT_ROOT  = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
BRONZE_PATH = f"{MOUNT_ROOT}/bronze"
SILVER_PATH = f"{MOUNT_ROOT}/silver"

# All valid event types — if a new type appears in the source, we catch it here
# rather than letting it silently flow into the fact table
VALID_EVENT_TYPES = {
    "HIRE", "PROMOTION", "TRANSFER", "SALARY_CHANGE", "RESIGNATION", "TERMINATION"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read and inspect bronze

# COMMAND ----------

df_events_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/employee_events")
print(f"bronze.employee_events: {df_events_bronze.count()} rows")
df_events_bronze.printSchema()
df_events_bronze.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean and cast

# COMMAND ----------

df_events_silver = (
    df_events_bronze

    # Trim all string columns
    .withColumn("event_id",             F.trim(F.col("event_id")))
    .withColumn("employee_id",          F.trim(F.col("employee_id")))
    .withColumn("event_type",           F.upper(F.trim(F.col("event_type"))))  # normalise to UPPER
    .withColumn("department_id",        F.trim(F.col("department_id")))
    .withColumn("job_role_id",          F.trim(F.col("job_role_id")))
    .withColumn("notes",                F.trim(F.col("notes")))

    # Empty string manager → null
    .withColumn("manager_employee_id",
        F.when(F.trim(F.col("manager_employee_id")) == "", None)
         .otherwise(F.trim(F.col("manager_employee_id")))
    )

    # Cast event_date from string to DateType
    # Bronze kept this as string to preserve any bad values — Silver casts it
    .withColumn("event_date", F.to_date(F.col("event_date"), "yyyy-MM-dd"))

    # Cast salary to IntegerType
    .withColumn("salary", F.col("salary").cast("integer"))

    .drop("_source_file", "_ingested_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate
# MAGIC
# MAGIC These checks raise exceptions on failure — not warnings, not prints.
# MAGIC WHY: If we just print a warning and continue, ADF will mark the run as
# MAGIC Succeeded even though bad data flowed into Gold. An exception causes
# MAGIC ADF to mark the run as Failed and stops the pipeline — which is exactly
# MAGIC what you want when data quality breaks.

# COMMAND ----------

# 1. No null event_dates (unparseable dates become null — catch them here)
bad_dates = df_events_silver.filter(F.col("event_date").isNull()).count()
if bad_dates > 0:
    raise ValueError(f"QUALITY FAIL: {bad_dates} rows with null/unparseable event_date.")

# 2. No null employee_ids
null_emp = df_events_silver.filter(F.col("employee_id").isNull()).count()
if null_emp > 0:
    raise ValueError(f"QUALITY FAIL: {null_emp} rows with null employee_id.")

# 3. Only known event_types allowed
unknown_types = (
    df_events_silver
    .filter(~F.col("event_type").isin(list(VALID_EVENT_TYPES)))
    .select("event_type").distinct()
)
if unknown_types.count() > 0:
    print("QUALITY FAIL: Unknown event_types found:")
    unknown_types.show()
    raise ValueError("Unknown event_type values found — update VALID_EVENT_TYPES or fix source data.")

# 4. No duplicate event_ids
dup_events = (
    df_events_silver.groupBy("event_id").agg(spark_count("*").alias("n"))
    .filter(F.col("n") > 1)
)
if dup_events.count() > 0:
    raise ValueError("QUALITY FAIL: Duplicate event_ids found.")

# 5. Every employee should have exactly one HIRE event (first event must be HIRE)
hire_counts = (
    df_events_silver
    .filter(F.col("event_type") == "HIRE")
    .groupBy("employee_id")
    .agg(spark_count("*").alias("hire_count"))
)
missing_hires = hire_counts.filter(F.col("hire_count") == 0).count()
multi_hires = hire_counts.filter(F.col("hire_count") > 1).count()
if multi_hires > 0:
    raise ValueError(f"QUALITY FAIL: {multi_hires} employees with more than one HIRE event.")

print("All validation checks passed.")
print()

# Summary stats
print("Event type distribution:")
df_events_silver.groupBy("event_type").agg(spark_count("*").alias("count")).orderBy("event_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write silver.employee_events

# COMMAND ----------

(
    df_events_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/employee_events")
)

final_count = spark.read.format("delta").load(f"{SILVER_PATH}/employee_events").count()
print(f"Written: silver/employee_events ({final_count} rows)")
print("Next: run 06_silver_contact_info.py")
