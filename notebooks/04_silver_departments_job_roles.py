# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Departments & Job Roles
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Cleans and validates the two static reference tables from bronze.
# MAGIC These are simple — no SCD2, no complex logic — but they establish
# MAGIC the cleaning pattern you'll see repeated across every silver notebook:
# MAGIC   1. Read from bronze Delta
# MAGIC   2. Trim whitespace, cast types, handle nulls
# MAGIC   3. Validate business rules (no nulls on key columns)
# MAGIC   4. Write to silver Delta
# MAGIC
# MAGIC **Why these tables are NOT SCD2:**
# MAGIC Departments and job roles change rarely — and when they do (a department
# MAGIC is renamed or restructured), we treat that as a correction to the reference
# MAGIC data, not a version. SCD2 is reserved for employee attributes where history
# MAGIC genuinely matters for fact table accuracy.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import count as spark_count

MOUNT_ROOT  = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
BRONZE_PATH = f"{MOUNT_ROOT}/bronze"
SILVER_PATH = f"{MOUNT_ROOT}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver.departments

# COMMAND ----------

df_dept_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/departments")
print(f"bronze.departments: {df_dept_bronze.count()} rows")
df_dept_bronze.show(truncate=False)

# COMMAND ----------

df_dept_silver = (
    df_dept_bronze
    .withColumn("department_id",   F.trim(F.col("department_id")))
    .withColumn("department_name", F.trim(F.col("department_name")))
    .withColumn("cost_center",     F.trim(F.col("cost_center")))
    .drop("_source_file", "_ingested_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# Validate: no nulls on primary key
nulls = df_dept_silver.filter(F.col("department_id").isNull()).count()
if nulls > 0:
    raise ValueError(f"{nulls} null department_id values found — aborting.")

# Validate: no duplicate department_ids
dups = (
    df_dept_silver.groupBy("department_id").agg(spark_count("*").alias("n"))
    .filter(F.col("n") > 1).count()
)
if dups > 0:
    raise ValueError(f"{dups} duplicate department_id values found — aborting.")

print(f"departments validated: {df_dept_silver.count()} rows, no nulls, no duplicates")
df_dept_silver.show(truncate=False)

# COMMAND ----------

(
    df_dept_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/departments")
)
print("Written: silver/departments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver.job_roles

# COMMAND ----------

df_roles_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/job_roles")
print(f"bronze.job_roles: {df_roles_bronze.count()} rows")
df_roles_bronze.show(truncate=False)

# COMMAND ----------

df_roles_silver = (
    df_roles_bronze
    .withColumn("job_role_id", F.trim(F.col("job_role_id")))
    .withColumn("job_title",   F.trim(F.col("job_title")))
    .withColumn("job_level",   F.trim(F.col("job_level")))
    .drop("_source_file", "_ingested_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# Validate
nulls = df_roles_silver.filter(F.col("job_role_id").isNull()).count()
if nulls > 0:
    raise ValueError(f"{nulls} null job_role_id values found — aborting.")

dups = (
    df_roles_silver.groupBy("job_role_id").agg(spark_count("*").alias("n"))
    .filter(F.col("n") > 1).count()
)
if dups > 0:
    raise ValueError(f"{dups} duplicate job_role_id values found — aborting.")

print(f"job_roles validated: {df_roles_silver.count()} rows, no nulls, no duplicates")
df_roles_silver.show(truncate=False)

# COMMAND ----------

(
    df_roles_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/job_roles")
)
print("Written: silver/job_roles")
print("Next: run 05_silver_employee_events.py")
