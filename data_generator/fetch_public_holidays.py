"""
fetch_public_holidays.py
=========================
Fetches public holidays from the Nager.Date public API and saves as JSON.
This script will be ported into a Databricks notebook for the bronze layer.

WHY NAGER.DATE:
  - Free, no API key required, stable REST API
  - Returns structured JSON natively (no HTML scraping)
  - Teaches the real-world pattern: external reference data your org doesn't own
    being pulled into your pipeline to enrich internal data (dim_date in Gold)
  - URL pattern: https://date.nager.at/api/v3/PublicHolidays/{year}/{countryCode}

WHAT THIS FEEDS IN GOLD:
  dim_date will have a column is_public_holiday (boolean) and holiday_name (string).
  The snapshot fact (fact_headcount_snapshot) covers Jan-Jun 2026, so we fetch
  2026 holidays for a couple of countries to keep it realistic and teachable.

COUNTRIES FETCHED (deliberately more than one to teach UNION and country filtering):
  - GB (United Kingdom)  -- Maersk has major offices there
  - US (United States)   -- common reference point everyone knows
  - IN (India)           -- broad recognition, diverse holiday calendar

API RESPONSE SHAPE (one record):
  {
    "date": "2026-01-01",
    "localName": "New Year's Day",
    "name": "New Year's Day",
    "countryCode": "GB",
    "fixed": true,
    "global": true,
    "counties": null,
    "launchYear": null,
    "types": ["Public"]
  }

TEACHING POINTS FOR BRONZE NOTEBOOK:
  1. Making an HTTP GET request from Databricks using the requests library
  2. Iterating over multiple API calls (one per country) and combining results
  3. Saving the combined JSON to ADLS bronze for later Delta conversion
  4. Why we save raw API response to bronze first rather than transforming inline
     (answer: bronze = source of truth, re-runnable without hitting API again)
"""

import json
import urllib.request   # stdlib only -- no pip needed, works in Databricks too
import urllib.error

YEAR = 2026
COUNTRY_CODES = ["GB", "US", "IN"]
BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"


def fetch_holidays_for_country(year: int, country_code: str) -> list:
    """
    Makes a single GET request to Nager.Date and returns the parsed JSON list.

    WHY urllib INSTEAD OF requests:
    urllib is Python stdlib -- zero dependencies, always available in any
    Databricks runtime without pip install. For simple GET requests returning
    JSON, it's perfectly sufficient and teaches the underlying HTTP mechanics
    more transparently than requests does.
    """
    url = f"{BASE_URL}/{year}/{country_code}"
    print(f"  Fetching: {url}")

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status != 200:
                raise ValueError(f"Unexpected HTTP status {response.status} for {country_code}")
            raw_bytes = response.read()
            holidays = json.loads(raw_bytes.decode("utf-8"))
            print(f"  -> {len(holidays)} holidays returned for {country_code}")
            return holidays

    except urllib.error.HTTPError as e:
        print(f"  ERROR: HTTP {e.code} for {country_code} -- skipping")
        return []
    except urllib.error.URLError as e:
        print(f"  ERROR: Could not reach API ({e.reason}) -- skipping")
        return []


def fetch_all_countries(year: int, country_codes: list) -> list:
    """
    Fetches holidays for all countries and returns a flat combined list.

    NOTE: We deliberately keep all countries in one file rather than separate
    files per country. This means the Bronze notebook has to handle a
    multi-country dataset from the start -- which is more realistic and
    teaches the 'filter by countryCode' pattern in Silver.
    """
    all_holidays = []
    for cc in country_codes:
        holidays = fetch_holidays_for_country(year, cc)
        all_holidays.extend(holidays)
    return all_holidays


if __name__ == "__main__":
    import os

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "public_holidays.json")

    print(f"Fetching {YEAR} public holidays for: {', '.join(COUNTRY_CODES)}")
    print()

    holidays = fetch_all_countries(YEAR, COUNTRY_CODES)

    if not holidays:
        print("WARNING: No holidays fetched. Check network connectivity.")
    else:
        with open(out_path, "w") as f:
            json.dump(holidays, f, indent=2)

        print()
        print(f"Wrote {len(holidays)} total holiday records -> {out_path}")
        print()
        print("Breakdown by country:")
        from collections import Counter
        counts = Counter(h["countryCode"] for h in holidays)
        for cc, n in sorted(counts.items()):
            print(f"  {cc}: {n} holidays")

        print()
        print("Sample record:")
        print(json.dumps(holidays[0], indent=2))
