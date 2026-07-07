# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Employees (SCD2)
# MAGIC
# MAGIC **This is the most important notebook in the pipeline.**
# MAGIC Everything downstream — dim_employee in Gold, both fact tables — depends
# MAGIC on getting this right.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads `bronze.employees` and `bronze.employee_events`
# MAGIC 2. Cleans and casts both sources
# MAGIC 3. Replays the event log chronologically to build full SCD2 history
# MAGIC 4. Writes `silver.employees_scd2` — one row per version of each employee
# MAGIC
# MAGIC **What SCD2 means (and why it matters):**
# MAGIC SCD = Slowly Changing Dimension. Type 2 = keep ALL historical versions,
# MAGIC not just the current one.
# MAGIC
# MAGIC Without SCD2: if an employee transferred from Engineering to Sales in March,
# MAGIC any report joining to dim_employee would show them in Sales for ALL months —
# MAGIC including January and February when they were in Engineering. Historical
# MAGIC headcount by department would be wrong.
# MAGIC
# MAGIC With SCD2: we have two rows for that employee:
# MAGIC   Row 1: dept=Engineering, valid_from=hire_date,  valid_to=2026-02-28, is_current=False
# MAGIC   Row 2: dept=Sales,       valid_from=2026-03-01, valid_to=9999-12-31, is_current=True
# MAGIC
# MAGIC Fact tables join to the version that was active at the time of the event.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, BooleanType, DoubleType
)
import hashlib

MOUNT_ROOT   = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
BRONZE_PATH  = f"{MOUNT_ROOT}/bronze"
SILVER_PATH  = f"{MOUNT_ROOT}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read bronze sources

# COMMAND ----------

df_emp_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/employees")
df_events_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/employee_events")

print(f"bronze.employees      : {df_emp_bronze.count()} rows")
print(f"bronze.employee_events: {df_events_bronze.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean and cast employees (from bronze.employees)
# MAGIC
# MAGIC Bronze preserved everything as raw strings (especially from Excel).
# MAGIC Silver's job is to:
# MAGIC   - Trim whitespace (those deliberate trailing spaces in email)
# MAGIC   - Cast columns to correct types
# MAGIC   - Handle nulls explicitly (empty string → null for termination_date)
# MAGIC   - Validate nothing critical is missing

# COMMAND ----------

df_emp_clean = (
    df_emp_bronze
    # Trim whitespace from all string columns — catches the deliberate
    # trailing-space emails baked into employees.xlsx
    .withColumn("employee_id",         F.trim(F.col("employee_id")))
    .withColumn("first_name",          F.trim(F.col("first_name")))
    .withColumn("last_name",           F.trim(F.col("last_name")))
    .withColumn("email",               F.trim(F.col("email")))   # <-- fixes trailing whitespace
    .withColumn("department_id",       F.trim(F.col("department_id")))
    .withColumn("job_role_id",         F.trim(F.col("job_role_id")))
    .withColumn("manager_employee_id", F.trim(F.col("manager_employee_id")))

    # Cast dates — Excel delivers these as strings, Silver makes them proper DateType
    .withColumn("hire_date",
        F.to_date(F.col("hire_date"), "yyyy-MM-dd"))

    # Empty string → null for termination_date (active employees have no termination)
    # Then cast to DateType
    .withColumn("termination_date",
        F.to_date(
            F.when(F.trim(F.col("termination_date")) == "", None)
             .otherwise(F.col("termination_date")),
            "yyyy-MM-dd"
        )
    )

    # Cast salary to integer (Excel may deliver as string or float)
    .withColumn("salary", F.col("salary").cast(IntegerType()))

    # Cast is_active to boolean
    .withColumn("is_active",
        F.when(F.upper(F.trim(F.col("is_active"))) == "TRUE", True)
         .otherwise(False)
         .cast(BooleanType())
    )

    # Drop bronze audit columns — Silver adds its own
    .drop("_source_file", "_ingested_at")
)

# Validate: no employee_id nulls allowed — these are our business keys
null_ids = df_emp_clean.filter(F.col("employee_id").isNull()).count()
if null_ids > 0:
    raise ValueError(f"Found {null_ids} rows with null employee_id in employees — aborting.")

print("employees cleaned and cast:")
df_emp_clean.printSchema()
df_emp_clean.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and cast employee_events
# MAGIC
# MAGIC The event log is what we'll replay to build SCD2 history.
# MAGIC Critical requirements:
# MAGIC   - event_date must be a valid DateType (was kept as string in bronze)
# MAGIC   - Events must be sorted chronologically per employee
# MAGIC   - No duplicate event_ids allowed

# COMMAND ----------

df_events_clean = (
    df_events_bronze
    .withColumn("event_id",             F.trim(F.col("event_id")))
    .withColumn("employee_id",          F.trim(F.col("employee_id")))
    .withColumn("event_type",           F.trim(F.col("event_type")))
    .withColumn("department_id",        F.trim(F.col("department_id")))
    .withColumn("job_role_id",          F.trim(F.col("job_role_id")))
    .withColumn("manager_employee_id",  F.trim(F.col("manager_employee_id")))
    .withColumn("notes",                F.trim(F.col("notes")))

    # Cast event_date — this was deliberately kept as string in bronze
    # to preserve bad values. Any unparseable dates become null here,
    # which we then catch with the validation below.
    .withColumn("event_date", F.to_date(F.col("event_date"), "yyyy-MM-dd"))

    # Empty string manager → null (root-level managers have no manager)
    .withColumn("manager_employee_id",
        F.when(F.col("manager_employee_id") == "", None)
         .otherwise(F.col("manager_employee_id"))
    )

    .drop("_source_file", "_ingested_at")
)

# Validate: no null event_dates (would break chronological replay)
bad_dates = df_events_clean.filter(F.col("event_date").isNull()).count()
if bad_dates > 0:
    raise ValueError(f"Found {bad_dates} rows with unparseable event_date — check source CSV.")

# Validate: no duplicate event_ids
from pyspark.sql.functions import count as spark_count
dup_events = (
    df_events_clean
    .groupBy("event_id")
    .agg(spark_count("*").alias("n"))
    .filter(F.col("n") > 1)
    .count()
)
if dup_events > 0:
    raise ValueError(f"Found {dup_events} duplicate event_ids — check source CSV.")

print(f"employee_events cleaned: {df_events_clean.count()} rows, 0 bad dates, 0 duplicate IDs")
df_events_clean.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build SCD2 history by replaying the event log
# MAGIC
# MAGIC **This is the core of the notebook. Read this carefully.**
# MAGIC
# MAGIC ### The strategy
# MAGIC
# MAGIC We have an event log where each row represents a state change for an employee.
# MAGIC We want to turn this into a "versions" table where each row says:
# MAGIC   "From date X to date Y, this employee had these attributes."
# MAGIC
# MAGIC The approach:
# MAGIC   1. Take every event per employee, sorted by event_date
# MAGIC   2. Each event row = the START of a new version
# MAGIC   3. The END of that version = the day before the NEXT event (using Window lead())
# MAGIC   4. The last version's end date = 9999-12-31 (the "still current" sentinel)
# MAGIC   5. Exclude EXIT events from the version table (exits close a version, not start one)
# MAGIC   6. Add a row hash (MD5) to detect which columns actually changed between versions
# MAGIC
# MAGIC ### Why 9999-12-31 as the open-ended date?
# MAGIC NULL for "still active" is tempting but dangerous — NULL = NULL is always
# MAGIC False in SQL, so joins on valid_to would silently fail for current records.
# MAGIC 9999-12-31 is a safe sentinel: it participates in BETWEEN comparisons correctly,
# MAGIC and it's visually obvious what it means.

# COMMAND ----------

# Filter to only state-bearing events (not EXIT events — those close a version)
# EXIT events tell us WHEN a version closed, not what the new state is.
EXIT_EVENTS = ["RESIGNATION", "TERMINATION"]

df_state_events = (
    df_events_clean
    .filter(~F.col("event_type").isin(EXIT_EVENTS))
    .select(
        "employee_id",
        "event_date",
        "event_type",
        "department_id",
        "job_role_id",
        "manager_employee_id",
        "salary"
    )
)

# Also capture exit dates separately — we'll use these to close the final version
df_exit_events = (
    df_events_clean
    .filter(F.col("event_type").isin(EXIT_EVENTS))
    .select(
        "employee_id",
        F.col("event_date").alias("exit_date"),
        F.col("event_type").alias("exit_type")
    )
)

print(f"State events (version starters): {df_state_events.count()}")
print(f"Exit events  (version closers) : {df_exit_events.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window function: lead() to find the next event date
# MAGIC
# MAGIC `lead(event_date, 1)` over a window partitioned by employee_id and ordered
# MAGIC by event_date gives us "the date of the next event for this employee."
# MAGIC
# MAGIC That next event date - 1 day = the end of the current version.
# MAGIC If there's no next event (lead returns null), this is the last version → 9999-12-31.

# COMMAND ----------

window_by_employee = Window.partitionBy("employee_id").orderBy("event_date")

df_versions = (
    df_state_events

    # valid_from = the event_date of this row (when this version starts)
    .withColumn("valid_from", F.col("event_date"))

    # valid_to = day before the next event. If no next event → 9999-12-31
    .withColumn("next_event_date", F.lead("event_date", 1).over(window_by_employee))
    .withColumn("valid_to",
        F.when(F.col("next_event_date").isNull(), F.lit("9999-12-31").cast(DateType()))
         .otherwise(F.date_sub(F.col("next_event_date"), 1))
    )

    .drop("next_event_date", "event_date", "event_type")
)

print("Versions before exit-date adjustment:")
df_versions.orderBy("employee_id", "valid_from").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adjust valid_to for exited employees
# MAGIC
# MAGIC For employees who resigned or were terminated, their last version's valid_to
# MAGIC should be their actual exit date — not 9999-12-31.
# MAGIC
# MAGIC Without this step, a terminated employee would appear as "still active"
# MAGIC in the last version, which would make the snapshot fact show them as
# MAGIC is_active=True in months after their exit. That's wrong.

# COMMAND ----------

df_versions_with_exits = (
    df_versions
    .join(df_exit_events, on="employee_id", how="left")

    # For the last version of an exited employee (valid_to = 9999-12-31),
    # replace with their actual exit_date
    .withColumn("valid_to",
        F.when(
            (F.col("exit_date").isNotNull()) &
            (F.col("valid_to") == F.lit("9999-12-31").cast(DateType())),
            F.col("exit_date")
        ).otherwise(F.col("valid_to"))
    )

    # is_current: True only if valid_to = 9999-12-31 (still active, no exit)
    .withColumn("is_current",
        F.col("valid_to") == F.lit("9999-12-31").cast(DateType())
    )

    .drop("exit_date", "exit_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add surrogate key and row hash
# MAGIC
# MAGIC **Surrogate key** (`employee_sk`):
# MAGIC A unique ID for each VERSION of an employee (not for the employee themselves).
# MAGIC The employee's business key is still `employee_id` — but in Gold, the fact
# MAGIC table will join to the specific version that was active at event time,
# MAGIC which requires a key that uniquely identifies each version row.
# MAGIC We build it as MD5(employee_id || valid_from) — deterministic, stable.
# MAGIC
# MAGIC **Row hash** (`row_hash`):
# MAGIC MD5 of the SCD2-tracked columns (dept, job, manager, salary).
# MAGIC Used in incremental loads to detect whether anything actually changed
# MAGIC between the current version and a new incoming record. If the hash matches,
# MAGIC no new version is needed. We build it now so the pattern is visible.
# MAGIC
# MAGIC **WHY `||` AS SEPARATOR IN THE HASH:**
# MAGIC Concatenating "EMP001" + "2026-01-01" gives "EMP0012026-01-01".
# MAGIC Concatenating "EMP00" + "12026-01-01" gives the same string — collision.
# MAGIC Inserting a separator that can't appear in either value ("||") prevents this:
# MAGIC "EMP001||2026-01-01" vs "EMP00||12026-01-01" — always distinct.

# COMMAND ----------

df_scd2 = (
    df_versions_with_exits

    # Surrogate key: unique per version (employee + which version start date)
    .withColumn("employee_sk",
        F.md5(F.concat_ws("||",
            F.col("employee_id"),
            F.col("valid_from").cast(StringType())
        ))
    )

    # Row hash: fingerprint of the SCD2-tracked attribute columns
    # If this hash hasn't changed vs. the previous version, no new row needed
    .withColumn("row_hash",
        F.md5(F.concat_ws("||",
            F.col("department_id"),
            F.col("job_role_id"),
            F.coalesce(F.col("manager_employee_id"), F.lit("NULL")),
            F.col("salary").cast(StringType())
        ))
    )

    # Add name and email from the cleaned employees table (not in events log)
    .join(
        df_emp_clean.select("employee_id", "first_name", "last_name", "email", "hire_date"),
        on="employee_id",
        how="left"
    )

    # Final column order — clear and teachable
    .select(
        "employee_sk",
        "employee_id",
        "first_name",
        "last_name",
        "email",
        "hire_date",
        "department_id",
        "job_role_id",
        "manager_employee_id",
        "salary",
        "valid_from",
        "valid_to",
        "is_current",
        "row_hash"
    )

    .withColumn("_silver_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate the SCD2 output
# MAGIC
# MAGIC Before writing, verify the output tells the right story.
# MAGIC These checks catch logic bugs that no schema validation would catch.

# COMMAND ----------

total_versions = df_scd2.count()
unique_employees = df_scd2.select("employee_id").distinct().count()
current_versions = df_scd2.filter(F.col("is_current") == True).count()

print(f"Total versions (rows)   : {total_versions}")
print(f"Unique employees        : {unique_employees}  (expect 18)")
print(f"Current versions        : {current_versions}  (expect 15 — 18 minus 3 exited)")
print()

# Each employee must have exactly one is_current=True version (or 0 if exited)
version_counts = (
    df_scd2
    .groupBy("employee_id")
    .agg(
        spark_count("*").alias("total_versions"),
        F.sum(F.col("is_current").cast(IntegerType())).alias("current_count")
    )
    .orderBy("employee_id")
)
print("Versions per employee:")
version_counts.show(20, truncate=False)

# valid_from must always be <= valid_to
bad_dates = df_scd2.filter(F.col("valid_from") > F.col("valid_to")).count()
if bad_dates > 0:
    raise ValueError(f"Found {bad_dates} rows where valid_from > valid_to — SCD2 logic error.")

# No duplicate surrogate keys
dup_sks = (
    df_scd2.groupBy("employee_sk").agg(spark_count("*").alias("n"))
    .filter(F.col("n") > 1).count()
)
if dup_sks > 0:
    raise ValueError(f"Found {dup_sks} duplicate employee_sk values — hash collision or data issue.")

print("All validation checks passed.")

# COMMAND ----------

# Show the full history for an employee with 2 changes — the clearest teaching example
print("Sample: full SCD2 history for an employee with multiple versions")
multi_version_emp = (
    version_counts.filter(F.col("total_versions") >= 3)
    .orderBy("employee_id")
    .first()["employee_id"]
)
df_scd2.filter(F.col("employee_id") == multi_version_emp).orderBy("valid_from").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write silver.employees_scd2

# COMMAND ----------

(
    df_scd2
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/employees_scd2")
)

final_count = spark.read.format("delta").load(f"{SILVER_PATH}/employees_scd2").count()
print(f"Written: silver/employees_scd2 ({final_count} version rows across {unique_employees} employees)")
print("Next: run 04_silver_departments.py")
