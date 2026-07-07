# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — JSON Sources
# MAGIC ### `employee_contact_info.json` · `public_holidays.json`
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Reads 2 JSON files from the ADLS landing zone and writes each to a
# MAGIC Delta table in the bronze layer — raw, no transformations.
# MAGIC
# MAGIC **Why a separate notebook from file sources?**
# MAGIC JSON introduces concepts that don't exist in flat CSV/Excel:
# MAGIC   - Nested structs (objects inside objects)
# MAGIC   - Arrays of structs (lists of objects)
# MAGIC   - Schema inference vs explicit schema on nested types
# MAGIC   - Null handling inside nested paths
# MAGIC
# MAGIC These are distinct enough to deserve focused attention rather than being
# MAGIC buried at the bottom of the file-sources notebook.
# MAGIC
# MAGIC **Bronze rule still applies:**
# MAGIC We preserve the raw nested structure in bronze — we do NOT flatten here.
# MAGIC Flattening nested JSON is Silver's job. Bronze just lands it as Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, ArrayType,
    StringType, BooleanType, LongType, TimestampType
)

MOUNT_ROOT   = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
LANDING_PATH = f"{MOUNT_ROOT}/bronze/landing"
BRONZE_PATH  = f"{MOUNT_ROOT}/bronze"

print(f"Landing zone : {LANDING_PATH}")
print(f"Bronze output: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify landing zone files are present

# COMMAND ----------

landing_files = [f.name for f in dbutils.fs.ls(LANDING_PATH)]

required_files = ["employee_contact_info.json", "public_holidays.json"]
missing = [f for f in required_files if f not in landing_files]
if missing:
    raise FileNotFoundError(f"Missing files in landing zone: {missing}")

print("Required JSON files confirmed present.")

# COMMAND ----------

def add_audit_columns(df, source_filename: str):
    return (
        df
        .withColumn("_source_file", F.lit(source_filename))
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. employee_contact_info.json → bronze.employee_contact_info
# MAGIC
# MAGIC ### Understanding the JSON shape first
# MAGIC
# MAGIC Before writing any read code, always understand what you're reading.
# MAGIC This file is a JSON array of 18 objects. Each object looks like:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "employee_id": "EMP001",
# MAGIC   "contact": {
# MAGIC     "phone": { "mobile": "...", "work": "...", "home": "..." },
# MAGIC     "email": { "personal": "...", "work": "..." }
# MAGIC   },
# MAGIC   "address": {
# MAGIC     "line1": "...", "line2": "...", "city": "...",
# MAGIC     "state_or_province": "...", "postal_code": "...", "country_code": "..."
# MAGIC   },
# MAGIC   "emergency_contact": {           <-- can be NULL entirely (EMP007)
# MAGIC     "full_name": "...",
# MAGIC     "relationship": "...",
# MAGIC     "contact": { "phone": "...", "email": "..." }
# MAGIC   },
# MAGIC   "preferences": [                 <-- array of structs, can be [] (EMP003/011/015)
# MAGIC     { "key": "notification_channel", "value": "sms" },
# MAGIC     ...
# MAGIC   ],
# MAGIC   "metadata": {
# MAGIC     "source_system": "...", "last_updated": "...", "schema_version": "..."
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC In bronze we keep this nested structure EXACTLY as-is.
# MAGIC Silver will flatten it into columns. The reason is the same as always:
# MAGIC if you flatten in bronze and later realise you need a field you dropped,
# MAGIC you have to go back to the source file. Keep bronze complete.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why we use spark.read.option("multiLine", "true") for this file
# MAGIC
# MAGIC PySpark's JSON reader by default expects one JSON object per line
# MAGIC (newline-delimited JSON / NDJSON format). Our file is a pretty-printed
# MAGIC JSON array spanning many lines — so we need multiLine=true to tell
# MAGIC Spark to read the whole file as one document, not one line at a time.
# MAGIC
# MAGIC Rule of thumb:
# MAGIC   - One JSON object per line  → multiLine = false (default, faster)
# MAGIC   - Pretty-printed / array    → multiLine = true  (needed here)

# COMMAND ----------

df_contact_raw = (
    spark.read
    .option("multiLine", "true")
    .json(f"{LANDING_PATH}/employee_contact_info.json")
)

print("Schema as inferred by Spark (nested structs preserved):")
df_contact_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you're seeing in printSchema()
# MAGIC
# MAGIC Notice the schema Spark inferred:
# MAGIC - `contact` is a `struct` (nested object) — access with dot notation: `contact.phone.mobile`
# MAGIC - `emergency_contact` is also a `struct` — but nullable because EMP007 has it as null
# MAGIC - `preferences` is an `array<struct<key,value>>` — needs `explode()` in Silver to unnest
# MAGIC - `metadata` is a `struct` — straightforward dot notation
# MAGIC
# MAGIC In Bronze we don't touch any of this. We just confirm Spark read it correctly
# MAGIC and write it to Delta. Silver does all the flattening.

# COMMAND ----------

# Row count check
row_count = df_contact_raw.count()
print(f"Row count: {row_count} (expect 18)")
if row_count != 18:
    raise ValueError(f"Expected 18 contact records, got {row_count}. Check the JSON file.")

# COMMAND ----------

# Quick sanity check: confirm the known quality issues landed correctly in bronze
# This is NOT cleaning — we're just verifying the raw data is intact as expected.

print("Quality issue spot-check (raw values, no cleaning):")
print()

df_contact_raw.createOrReplaceTempView("contact_raw_check")

# EMP007 should have null emergency_contact
spark.sql("""
    SELECT employee_id, emergency_contact
    FROM contact_raw_check
    WHERE employee_id = 'EMP007'
""").show(truncate=False)

# EMP013 should have 'NOT-A-NUMBER' as mobile
spark.sql("""
    SELECT employee_id, contact.phone.mobile AS mobile_phone
    FROM contact_raw_check
    WHERE employee_id = 'EMP013'
""").show(truncate=False)

# EMP016 should have null work email
spark.sql("""
    SELECT employee_id, contact.email.work AS work_email
    FROM contact_raw_check
    WHERE employee_id = 'EMP016'
""").show(truncate=False)

# EMP003/011/015 should have empty preferences arrays
spark.sql("""
    SELECT employee_id, size(preferences) AS pref_count
    FROM contact_raw_check
    WHERE employee_id IN ('EMP003', 'EMP011', 'EMP015')
""").show(truncate=False)

# COMMAND ----------

df_contact = add_audit_columns(df_contact_raw, "employee_contact_info.json")

(
    df_contact
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/employee_contact_info")
)

print(f"Written: bronze/employee_contact_info ({row_count} rows, nested structs preserved)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. public_holidays.json → bronze.public_holidays
# MAGIC
# MAGIC ### Understanding the shape
# MAGIC
# MAGIC This file is also a JSON array, but simpler — mostly flat fields.
# MAGIC One interesting field: `types` is an array of strings (e.g. `["Public"]`).
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "date": "2026-01-01",
# MAGIC   "localName": "New Year's Day",
# MAGIC   "name": "New Year's Day",
# MAGIC   "countryCode": "GB",
# MAGIC   "fixed": true,
# MAGIC   "global": true,
# MAGIC   "counties": null,
# MAGIC   "launchYear": null,
# MAGIC   "types": ["Public"]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Bronze keeps `types` as an array. Silver will decide whether to explode it
# MAGIC or just extract the first element (for dim_date enrichment, first element
# MAGIC is sufficient — a holiday is "Public" or it isn't).
# MAGIC
# MAGIC ### Live API call note
# MAGIC This file was pre-generated to match the exact Nager.Date API response shape.
# MAGIC In a production setup, this notebook would call the API directly using:
# MAGIC
# MAGIC ```python
# MAGIC import urllib.request, json
# MAGIC url = "https://date.nager.at/api/v3/PublicHolidays/2026/GB"
# MAGIC with urllib.request.urlopen(url) as r:
# MAGIC     holidays = json.loads(r.read())
# MAGIC ```
# MAGIC
# MAGIC The file-read approach and the live API approach produce identical DataFrames
# MAGIC downstream — the rest of the notebook is the same either way.

# COMMAND ----------

df_holidays_raw = (
    spark.read
    .option("multiLine", "true")
    .json(f"{LANDING_PATH}/public_holidays.json")
)

print("Schema:")
df_holidays_raw.printSchema()
# Note: 'types' will show as array<string> — this is correct and expected

# COMMAND ----------

row_count = df_holidays_raw.count()
print(f"Row count: {row_count}")
print()

# Show the country breakdown directly from the raw data
print("Holiday count by country:")
df_holidays_raw.groupBy("countryCode").count().orderBy("countryCode").show()

df_holidays_raw.show(truncate=False)

# COMMAND ----------

df_holidays = add_audit_columns(df_holidays_raw, "public_holidays.json")

(
    df_holidays
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{BRONZE_PATH}/public_holidays")
)

print(f"Written: bronze/public_holidays ({row_count} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final verification — all 6 bronze tables

# COMMAND ----------

all_bronze_tables = [
    "employees",
    "departments",
    "job_roles",
    "employee_events",
    "employee_contact_info",
    "public_holidays",
]

print("Full bronze layer verification:")
print("-" * 50)
all_ok = True
for table in all_bronze_tables:
    path = f"{BRONZE_PATH}/{table}"
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  {table:<28} {count:>4} rows  OK")
    except Exception as e:
        print(f"  {table:<28} FAILED: {e}")
        all_ok = False

print("-" * 50)
if all_ok:
    print("All 6 bronze tables verified. Ready for Silver layer.")
    print("Next: run 03_silver_employees.py")
else:
    print("One or more tables failed — review errors above before proceeding.")
