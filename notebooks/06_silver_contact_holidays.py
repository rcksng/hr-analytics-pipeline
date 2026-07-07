# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Contact Info & Public Holidays (JSON Flattening)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Flattens the deeply nested JSON structures from bronze into clean, column-per-field
# MAGIC silver tables that Gold can join to directly.
# MAGIC
# MAGIC **This is where the JSON teaching payoff happens.**
# MAGIC Bronze preserved the nested structs exactly as received.
# MAGIC Silver now has three distinct flattening problems to solve:
# MAGIC
# MAGIC   1. Nested struct → dot notation    (`contact.phone.mobile` → `mobile_phone`)
# MAGIC   2. Null top-level struct            (EMP007 has no emergency_contact at all)
# MAGIC   3. Array of structs → pivot columns (preferences [{key,value}] → individual columns)
# MAGIC   4. Malformed value handling         (EMP013 mobile = "NOT-A-NUMBER" → null + flag)
# MAGIC   5. Null scalar inside present struct (EMP016 work_email = null → handle gracefully)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import count as spark_count

MOUNT_ROOT  = "/mnt/adls_dev_bdi/rocky/hr-analytics-pipeline"
BRONZE_PATH = f"{MOUNT_ROOT}/bronze"
SILVER_PATH = f"{MOUNT_ROOT}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver.employee_contact_info
# MAGIC
# MAGIC ### Step 1: read bronze (nested structs intact)

# COMMAND ----------

df_contact_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/employee_contact_info")
print(f"bronze.employee_contact_info: {df_contact_bronze.count()} rows")
print("\nSchema (nested — this is what we're flattening):")
df_contact_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: flatten contact and address structs
# MAGIC
# MAGIC Dot notation (`contact.phone.mobile`) navigates into a nested struct.
# MAGIC This is exactly like selecting a nested field in JSON.
# MAGIC Each dot goes one level deeper.

# COMMAND ----------

df_contact_flat = (
    df_contact_bronze
    .select(
        # Business key — joins back to dim_employee
        F.col("employee_id"),

        # contact.phone — two levels deep
        F.col("contact.phone.mobile").alias("mobile_phone"),
        F.col("contact.phone.work").alias("work_phone"),
        F.col("contact.phone.home").alias("home_phone"),

        # contact.email — two levels deep
        F.col("contact.email.personal").alias("personal_email"),
        F.col("contact.email.work").alias("work_email"),

        # address — one level deep
        F.col("address.line1").alias("address_line1"),
        F.col("address.line2").alias("address_line2"),
        F.col("address.city").alias("city"),
        F.col("address.state_or_province").alias("state_or_province"),
        F.col("address.postal_code").alias("postal_code"),
        F.col("address.country_code").alias("country_code"),

        # emergency_contact — two levels deep, entire struct can be null (EMP007)
        # When the parent struct is null, dot notation returns null gracefully
        # for all child fields — no error raised, just null propagation
        F.col("emergency_contact.full_name").alias("ec_full_name"),
        F.col("emergency_contact.relationship").alias("ec_relationship"),
        F.col("emergency_contact.contact.phone").alias("ec_phone"),
        F.col("emergency_contact.contact.email").alias("ec_email"),

        # metadata
        F.col("metadata.source_system").alias("source_system"),
        F.col("metadata.last_updated").alias("last_updated"),

        # Keep preferences as-is for now — we handle it separately below
        F.col("preferences"),

        F.col("_ingested_at")
    )
)

print("Flattened schema:")
df_contact_flat.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: handle the malformed mobile phone (EMP013)
# MAGIC
# MAGIC "NOT-A-NUMBER" is not a valid phone number. In Silver we:
# MAGIC   1. Set the value to null (can't use it as-is)
# MAGIC   2. Add a flag column `mobile_phone_invalid` so downstream consumers
# MAGIC      know WHY it's null — not because it was missing from source,
# MAGIC      but because it was present but malformed.
# MAGIC
# MAGIC WHY FLAG INSTEAD OF JUST NULLING:
# MAGIC Null has two different meanings: "not provided" vs "provided but invalid."
# MAGIC Collapsing both into null loses that distinction. A flag column preserves it.
# MAGIC In a real pipeline you'd also route flagged records to a quarantine table
# MAGIC and alert the source system owner.

# COMMAND ----------

# Simple phone validation: a valid phone has at least some digits
# Real production would use a regex — keeping this simple for teaching
def is_invalid_phone(col_name):
    return (
        F.col(col_name).isNotNull() &
        ~F.col(col_name).rlike(r".*\d.*")  # no digits at all = invalid
    )

df_contact_cleaned = (
    df_contact_flat

    # Flag then null the malformed mobile
    .withColumn("mobile_phone_invalid",
        F.when(is_invalid_phone("mobile_phone"), F.lit(True))
         .otherwise(F.lit(False))
    )
    .withColumn("mobile_phone",
        F.when(F.col("mobile_phone_invalid"), None)
         .otherwise(F.col("mobile_phone"))
    )
)

# Verify the flag caught EMP013 correctly
print("EMP013 (expect mobile_phone=null, mobile_phone_invalid=True):")
df_contact_cleaned.filter(F.col("employee_id") == "EMP013") \
    .select("employee_id", "mobile_phone", "mobile_phone_invalid").show()

print("EMP007 (expect all ec_* columns = null, no error):")
df_contact_cleaned.filter(F.col("employee_id") == "EMP007") \
    .select("employee_id", "ec_full_name", "ec_relationship", "ec_phone", "ec_email").show()

print("EMP016 (expect work_email = null):")
df_contact_cleaned.filter(F.col("employee_id") == "EMP016") \
    .select("employee_id", "personal_email", "work_email").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: flatten the preferences array
# MAGIC
# MAGIC `preferences` is `array<struct<key:string, value:string>>`.
# MAGIC Three employees have an empty array `[]`.
# MAGIC
# MAGIC **Strategy: pivot from [{key,value}] to named columns**
# MAGIC Rather than exploding (which creates multiple rows per employee and would
# MAGIC break the one-row-per-employee structure we need for joining), we use
# MAGIC `filter + element_at` to extract each preference by key name.
# MAGIC
# MAGIC This keeps the table at 18 rows and creates three readable columns:
# MAGIC   pref_notification_channel, pref_language, pref_timezone
# MAGIC
# MAGIC For employees with empty preferences, these columns will be null — which
# MAGIC is correct and joinable in Gold without any special handling.

# COMMAND ----------

def extract_pref(array_col, key_name):
    """
    Extracts the 'value' from a [{key, value}] array where key = key_name.
    Returns null if the key is not present or array is empty.
    """
    return F.filter(array_col, lambda x: x["key"] == key_name)[0]["value"]

df_contact_silver = (
    df_contact_cleaned

    .withColumn("pref_notification_channel", extract_pref(F.col("preferences"), "notification_channel"))
    .withColumn("pref_language",             extract_pref(F.col("preferences"), "preferred_language"))
    .withColumn("pref_timezone",             extract_pref(F.col("preferences"), "timezone"))

    # Drop the raw preferences array — we've extracted what we need
    .drop("preferences", "_ingested_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

print("Final schema after flattening:")
df_contact_silver.printSchema()

print("\nFull table (18 rows, all flat):")
df_contact_silver.show(truncate=False)

# COMMAND ----------

# Validate row count preserved (no accidental explode/cross-join inflation)
row_count = df_contact_silver.count()
if row_count != 18:
    raise ValueError(f"Expected 18 rows after flattening, got {row_count} — likely accidental explode.")

print(f"Row count validated: {row_count} (correct)")

# COMMAND ----------

(
    df_contact_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/employee_contact_info")
)
print("Written: silver/employee_contact_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver.public_holidays
# MAGIC
# MAGIC Much simpler than contact info — mostly flat already.
# MAGIC Main tasks: cast date, extract first element from `types` array,
# MAGIC filter to our pipeline window (Jan-Jun 2026).

# COMMAND ----------

df_holidays_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/public_holidays")
print(f"bronze.public_holidays: {df_holidays_bronze.count()} rows")
df_holidays_bronze.printSchema()
df_holidays_bronze.show(truncate=False)

# COMMAND ----------

df_holidays_silver = (
    df_holidays_bronze

    # Cast date string to DateType
    .withColumn("holiday_date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    # Extract first element from types array — e.g. ["Public"] → "Public"
    # element_at is 1-indexed in Spark (not 0-indexed like Python)
    .withColumn("holiday_type", F.element_at(F.col("types"), 1))

    # Filter to our pipeline window only
    .filter(
        (F.col("holiday_date") >= F.lit("2026-01-01").cast("date")) &
        (F.col("holiday_date") <= F.lit("2026-06-30").cast("date"))
    )

    .select(
        F.col("holiday_date"),
        F.col("name").alias("holiday_name"),
        F.col("localName").alias("local_name"),
        F.col("countryCode").alias("country_code"),
        F.col("fixed"),
        F.col("global"),
        F.col("holiday_type")
    )

    .withColumn("_silver_processed_at", F.current_timestamp())
)

print(f"Holidays in pipeline window (Jan-Jun 2026): {df_holidays_silver.count()}")
df_holidays_silver.orderBy("country_code", "holiday_date").show(truncate=False)

# COMMAND ----------

(
    df_holidays_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{SILVER_PATH}/public_holidays")
)
print("Written: silver/public_holidays")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Full silver layer verification

# COMMAND ----------

silver_tables = [
    "employees_scd2",
    "departments",
    "job_roles",
    "employee_events",
    "employee_contact_info",
    "public_holidays",
]

print("Silver layer verification:")
print("-" * 50)
for table in silver_tables:
    path = f"{SILVER_PATH}/{table}"
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  {table:<28} {count:>4} rows  OK")
    except Exception as e:
        print(f"  {table:<28} FAILED: {e}")

print("-" * 50)
print("Silver layer complete. Next: build Gold layer notebooks.")
