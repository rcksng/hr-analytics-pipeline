"""
generate_contact_data.py
=========================
Generates employee_contact_info.json — a deeply nested JSON file that simulates
what a real HRIS REST API (like Workday or SAP SuccessFactors) would return when
you call something like GET /api/v1/employees/{id}/contact-profile.

WHY JSON AND NOT ANOTHER CSV:
Most enterprise APIs return JSON, not flat tables. The nesting is not cosmetic --
it reflects real-world structure where a single employee record contains:
  - A 'contact' sub-object (multiple phone/email channels)
  - An 'address' sub-object (structured location data)
  - An 'emergency_contact' sub-object (another person, nested inside the employee)
  - A 'preferences' array (a list of key-value pairs -- a different shape again)

TEACHING POINTS THIS FILE IS DESIGNED TO DEMONSTRATE IN BRONZE/SILVER:
  1. Exploding a JSON array into rows (spark.read.json + schema inference)
  2. Navigating nested structs with dot notation  (contact.phone.mobile)
  3. Flattening a nested object into columns     (address.city -> city)
  4. Handling a nested array of objects          (preferences[*])
  5. Dealing with nulls in nested paths          (some emergency_contacts missing)
  6. Catching malformed values in Silver         (one bad phone number format)

DELIBERATE QUALITY ISSUES (Silver's job to catch and handle):
  - EMP007: emergency_contact is entirely null  (missing field)
  - EMP013: mobile phone is "NOT-A-NUMBER"      (malformed format)
  - EMP016: work_email is null                  (missing value in nested object)
  - Three employees: preferences array is empty []  (valid JSON, edge case for explode)
"""

import json
import random
from faker import Faker

fake = Faker()
Faker.seed(99)   # different seed from employees so names don't accidentally match
random.seed(99)

EMPLOYEE_IDS = [f"EMP{str(i).zfill(3)}" for i in range(1, 19)]

NOTIFICATION_PREFS = ["email", "sms", "push", "none"]
LANGUAGE_PREFS = ["en", "de", "fr", "zh", "ar"]
TIMEZONE_PREFS = ["UTC", "America/New_York", "Europe/London", "Asia/Singapore", "Asia/Dubai"]

# Employees with deliberate quality issues -- fixed, not random,
# so the teaching story is stable every time this script is re-run.
MISSING_EMERGENCY_CONTACT = {"EMP007"}
MALFORMED_PHONE = {"EMP013"}
NULL_WORK_EMAIL = {"EMP016"}
EMPTY_PREFERENCES = {"EMP003", "EMP011", "EMP015"}


def build_contact(emp_id):
    mobile = fake.phone_number()
    if emp_id in MALFORMED_PHONE:
        mobile = "NOT-A-NUMBER"   # deliberate bad value -- Silver should flag this

    work_email = f"{emp_id.lower()}@hrpipeline-corp.com"
    if emp_id in NULL_WORK_EMAIL:
        work_email = None         # deliberate null -- Silver should handle this

    return {
        "phone": {
            "mobile": mobile,
            "work": fake.phone_number(),
            "home": fake.phone_number() if random.random() > 0.3 else None
        },
        "email": {
            "personal": fake.email(),
            "work": work_email
        }
    }


def build_address():
    return {
        "line1": fake.street_address(),
        "line2": fake.secondary_address() if random.random() > 0.5 else None,
        "city": fake.city(),
        "state_or_province": fake.state(),
        "postal_code": fake.postcode(),
        "country_code": fake.country_code()
    }


def build_emergency_contact(emp_id):
    """
    Returns a nested person object -- this is the trickiest structure to flatten
    in Bronze/Silver because it's a full object (not just a scalar value) nested
    inside the parent employee record.

    EMP007 gets None entirely -- tests how the Silver notebook handles a completely
    missing nested object (vs just a null scalar field inside a present object).
    Those are two different problems in PySpark schema handling.
    """
    if emp_id in MISSING_EMERGENCY_CONTACT:
        return None   # entire sub-object missing -- different from a null field inside it

    return {
        "full_name": fake.name(),
        "relationship": random.choice(["Spouse", "Parent", "Sibling", "Friend"]),
        "contact": {
            "phone": fake.phone_number(),
            "email": fake.email() if random.random() > 0.4 else None
        }
    }


def build_preferences(emp_id):
    """
    Returns a list of {key, value} objects -- an array of structs in Spark terms.
    This is a different shape from the nested objects above:
      - Nested object -> select(col("address.city")) -- dot notation
      - Array of structs -> explode(col("preferences")) -- needs explode first

    Three employees get an empty list [] to test edge cases in explode logic.
    """
    if emp_id in EMPTY_PREFERENCES:
        return []   # valid JSON but empty -- explode must not fail on this

    return [
        {"key": "notification_channel", "value": random.choice(NOTIFICATION_PREFS)},
        {"key": "preferred_language",   "value": random.choice(LANGUAGE_PREFS)},
        {"key": "timezone",             "value": random.choice(TIMEZONE_PREFS)},
    ]


def build_employee_contact_record(emp_id):
    """
    Top-level record shape -- this is what one 'API response object' looks like.
    The full file is a JSON array of 18 of these.

    Shape summary:
      employee_id          : string       (the business key, joins back to employees.xlsx)
      contact              : struct       (phone + email sub-objects)
        phone              : struct       (mobile, work, home)
        email              : struct       (personal, work)
      address              : struct       (line1, line2, city, state, postal, country)
      emergency_contact    : struct|null  (full_name, relationship, contact struct)
        contact            : struct       (phone, email)
      preferences          : array<struct> [{key, value}, ...]
      metadata             : struct       (source, last_updated -- mimics API envelope fields)
    """
    return {
        "employee_id": emp_id,
        "contact": build_contact(emp_id),
        "address": build_address(),
        "emergency_contact": build_emergency_contact(emp_id),
        "preferences": build_preferences(emp_id),
        "metadata": {
            "source_system": "HRIS-ContactAPI-v2",
            "last_updated": fake.date_time_between(
                start_date="-6m", end_date="now"
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": "2.1"
        }
    }


if __name__ == "__main__":
    import os
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "employee_contact_info.json")

    records = [build_employee_contact_record(emp_id) for emp_id in EMPLOYEE_IDS]

    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)

    print(f"Wrote {len(records)} records -> {out_path}")
    print("\nQuality issues baked in (Silver's job to catch):")
    print(f"  EMP007 : emergency_contact = null (entire sub-object missing)")
    print(f"  EMP013 : contact.phone.mobile = 'NOT-A-NUMBER' (malformed)")
    print(f"  EMP016 : contact.email.work = null (missing work email)")
    print(f"  EMP003, EMP011, EMP015 : preferences = [] (empty array)")
    print("\nSample record (EMP001):")
    print(json.dumps(records[0], indent=2))
