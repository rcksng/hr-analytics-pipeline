"""
generate_hr_data.py
====================
Generates synthetic HR data for the hr-analytics-pipeline teaching project.

WHY THIS SCRIPT EXISTS:
Real HR data is sensitive (PII, salaries, terminations) so we can never use
real company data in a public/teaching portfolio. This script builds a small,
believable HR dataset that mirrors real-world messiness:
  - Employees change department / job / salary / manager over time (SCD2 fuel)
  - Some employees exit mid-period (tests is_active logic in the gold fact)
  - Departments and job roles are static reference data (simple dimensions)
  - Events are logged one row per change (mirrors a real HRIS audit trail)

OUTPUT FILES (landed as "bronze" sources, one per real-world ingestion pattern):
  1. employees.xlsx        -> Excel source  (simulates HRBP-maintained roster)
  2. departments.csv       -> CSV source    (simulates static reference export)
  3. job_roles.csv         -> CSV source    (simulates static reference export)
  4. employee_events.csv   -> CSV source    (simulates HRIS audit/event log)

(employee_contact_info and public_holidays are NOT generated here -- those
come from live REST APIs, built in a separate notebook/script, to teach the
API ingestion pattern separately.)

DESIGN DECISIONS (worth understanding, not just running):
  - 18 employees, 4 departments, 6 months of history (Jan-Jun 2026)
  - ~50% of employees get exactly 1 change event (clean single SCD2 version)
  - ~20% get 2 change events (proves the SCD2 pattern generalizes to N versions)
  - Rest stay static (the "no history" baseline case)
  - 3 employees exit mid-period with a termination_date (tests is_active)
  - Employee IDs are stable "business keys" (EMP001, EMP002...) -- these are
    NOT the same as the surrogate keys we'll generate later in Silver/Gold.
    This distinction (business key vs surrogate key) is a core teaching point.
"""

import csv
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# CONFIG -- change these if you want a bigger/smaller dataset later
# ---------------------------------------------------------------------------
NUM_EMPLOYEES = 18
PERIOD_START = date(2026, 1, 1)
PERIOD_END = date(2026, 6, 30)

DEPARTMENTS = [
    {"department_id": "DPT01", "department_name": "Engineering", "cost_center": "CC-100"},
    {"department_id": "DPT02", "department_name": "Sales", "cost_center": "CC-200"},
    {"department_id": "DPT03", "department_name": "HR", "cost_center": "CC-300"},
    {"department_id": "DPT04", "department_name": "Finance", "cost_center": "CC-400"},
]

JOB_ROLES = [
    {"job_role_id": "JR01", "job_title": "Analyst", "job_level": "L1"},
    {"job_role_id": "JR02", "job_title": "Senior Analyst", "job_level": "L2"},
    {"job_role_id": "JR03", "job_title": "Manager", "job_level": "L3"},
    {"job_role_id": "JR04", "job_title": "Director", "job_level": "L4"},
    {"job_role_id": "JR05", "job_title": "Associate", "job_level": "L1"},
]

MANAGER_POOL = ["EMP001", "EMP002", "EMP003", "EMP004"]  # senior folks act as managers


def random_date_in_period(start=PERIOD_START, end=PERIOD_END):
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def generate_employees_and_events():
    """
    Builds two related datasets in lockstep:
      - employees: current-state snapshot (as if exported from HRIS today)
      - employee_events: every hire/promotion/transfer/salary_change/exit,
        in chronological order, which Silver will replay to build SCD2 history.

    Returns (employees_rows, events_rows)
    """
    employees_rows = []
    events_rows = []
    event_id_counter = 1

    # Decide employee "change profiles" up front so the mix is deliberate,
    # not accidental: ~50% one change, ~20% two changes, rest static.
    employee_ids = [f"EMP{str(i).zfill(3)}" for i in range(1, NUM_EMPLOYEES + 1)]
    shuffled = employee_ids.copy()
    random.shuffle(shuffled)

    n_two_changes = round(NUM_EMPLOYEES * 0.20)   # ~3-4 people
    n_one_change = round(NUM_EMPLOYEES * 0.50)    # ~9 people
    two_change_ids = set(shuffled[:n_two_changes])
    one_change_ids = set(shuffled[n_two_changes:n_two_changes + n_one_change])
    # everyone else = static (no change events at all)

    # Pick 3 employees to exit mid-period (excluding the manager pool, so
    # reporting lines don't break)
    exit_candidates = [e for e in employee_ids if e not in MANAGER_POOL]
    exit_ids = set(random.sample(exit_candidates, 3))
    # Fix the exit TYPE per employee once, deliberately, rather than rolling
    # it randomly later -- guarantees the dataset always tells the same
    # teaching story: one voluntary resignation, one involuntary termination,
    # one of either (kept random for variety across generator re-runs).
    exit_ids_list = list(exit_ids)
    exit_type_by_emp = {
        exit_ids_list[0]: "RESIGNATION",
        exit_ids_list[1]: "TERMINATION",
        exit_ids_list[2]: random.choice(["RESIGNATION", "TERMINATION"]),
    }

    for emp_id in employee_ids:
        first_name = fake.first_name()
        last_name = fake.last_name()
        hire_date = PERIOD_START - timedelta(days=random.randint(30, 900))  # hired before period, mostly
        # a couple of employees hired DURING the period (fresh hires)
        if random.random() < 0.15:
            hire_date = random_date_in_period(PERIOD_START, PERIOD_END - timedelta(days=30))

        dept = random.choice(DEPARTMENTS)
        job = random.choice(JOB_ROLES)
        manager_id = random.choice(MANAGER_POOL) if emp_id not in MANAGER_POOL else None
        base_salary = random.randint(45000, 65000) if job["job_level"] == "L1" else \
                      random.randint(65000, 90000) if job["job_level"] == "L2" else \
                      random.randint(90000, 120000) if job["job_level"] == "L3" else \
                      random.randint(120000, 160000)

        # --- HIRE event (every employee has one -- this is version 1 in SCD2 terms)
        current_state = {
            "employee_id": emp_id,
            "department_id": dept["department_id"],
            "job_role_id": job["job_role_id"],
            "manager_employee_id": manager_id,
            "salary": base_salary,
        }
        events_rows.append({
            "event_id": f"EVT{str(event_id_counter).zfill(4)}",
            "employee_id": emp_id,
            "event_type": "HIRE",
            "event_date": hire_date.isoformat(),
            "department_id": dept["department_id"],
            "job_role_id": job["job_role_id"],
            "manager_employee_id": manager_id or "",
            "salary": base_salary,
            "notes": "Initial hire record",
        })
        event_id_counter += 1

        # --- CHANGE events, based on this employee's assigned profile
        num_changes = 2 if emp_id in two_change_ids else (1 if emp_id in one_change_ids else 0)
        last_event_date = max(hire_date, PERIOD_START)

        for change_num in range(num_changes):
            # space changes out realistically within the period
            earliest = last_event_date + timedelta(days=20)
            if earliest >= PERIOD_END:
                break
            event_date = random_date_in_period(earliest, PERIOD_END)
            last_event_date = event_date

            change_type = random.choice(["PROMOTION", "TRANSFER", "SALARY_CHANGE"])

            if change_type == "PROMOTION":
                # bump job level if possible
                current_level_idx = ["L1", "L2", "L3", "L4"].index(
                    next(j["job_level"] for j in JOB_ROLES if j["job_role_id"] == current_state["job_role_id"])
                )
                if current_level_idx < 3:
                    new_job = next(j for j in JOB_ROLES if j["job_level"] == ["L1", "L2", "L3", "L4"][current_level_idx + 1])
                    current_state["job_role_id"] = new_job["job_role_id"]
                    current_state["salary"] = int(current_state["salary"] * 1.15)
                    notes = f"Promoted to {new_job['job_title']}"
                else:
                    change_type = "SALARY_CHANGE"
                    current_state["salary"] = int(current_state["salary"] * 1.08)
                    notes = "Merit salary increase"
            elif change_type == "TRANSFER":
                new_dept = random.choice([d for d in DEPARTMENTS if d["department_id"] != current_state["department_id"]])
                current_state["department_id"] = new_dept["department_id"]
                notes = f"Transferred to {new_dept['department_name']}"
            else:  # SALARY_CHANGE
                current_state["salary"] = int(current_state["salary"] * random.uniform(1.03, 1.10))
                notes = "Merit salary increase"

            events_rows.append({
                "event_id": f"EVT{str(event_id_counter).zfill(4)}",
                "employee_id": emp_id,
                "event_type": change_type,
                "event_date": event_date.isoformat(),
                "department_id": current_state["department_id"],
                "job_role_id": current_state["job_role_id"],
                "manager_employee_id": current_state["manager_employee_id"] or "",
                "salary": current_state["salary"],
                "notes": notes,
            })
            event_id_counter += 1

        # --- EXIT event, for the 3 chosen employees
        # NOTE: we GUARANTEE all 3 chosen employees actually exit (no silent
        # drops). If their last change event left too little room before
        # PERIOD_END, we simply place the exit close to PERIOD_END itself
        # rather than skipping it -- a data generator that silently drops
        # a promised scenario is worse than one that places it a bit tight.
        termination_date = None
        if emp_id in exit_ids:
            earliest_exit = last_event_date + timedelta(days=15)
            if earliest_exit >= PERIOD_END:
                earliest_exit = PERIOD_END - timedelta(days=3)
            termination_date = random_date_in_period(earliest_exit, PERIOD_END)
            exit_type = exit_type_by_emp[emp_id]  # fixed per employee, not re-rolled
            events_rows.append({
                "event_id": f"EVT{str(event_id_counter).zfill(4)}",
                "employee_id": emp_id,
                "event_type": exit_type,
                "event_date": termination_date.isoformat(),
                "department_id": current_state["department_id"],
                "job_role_id": current_state["job_role_id"],
                "manager_employee_id": current_state["manager_employee_id"] or "",
                "salary": current_state["salary"],
                "notes": "Employee exit",
            })
            event_id_counter += 1

        # --- Final "current state" row for the employees.xlsx roster export
        # (Intentional data-quality issue: ~10% chance email has trailing whitespace,
        #  ~10% chance phone is missing -- for Silver-layer cleaning practice)
        email = f"{first_name.lower()}.{last_name.lower()}@hrpipeline-demo.com"
        if random.random() < 0.10:
            email = email + "  "  # trailing whitespace, cleaned in Silver

        employees_rows.append({
            "employee_id": emp_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "hire_date": hire_date.isoformat(),
            "termination_date": termination_date.isoformat() if termination_date else "",
            "department_id": current_state["department_id"],
            "job_role_id": current_state["job_role_id"],
            "manager_employee_id": current_state["manager_employee_id"] or "",
            "salary": current_state["salary"],
            "is_active": "FALSE" if termination_date else "TRUE",
        })

    # sort events chronologically -- this matters for Silver SCD2 replay logic
    events_rows.sort(key=lambda r: (r["event_date"], r["employee_id"]))

    return employees_rows, events_rows


def write_csv(rows, filepath, fieldnames):
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {filepath}")


def write_employees_excel(rows, filepath):
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "employees"
    fieldnames = list(rows[0].keys())
    ws.append(fieldnames)
    for row in rows:
        ws.append([row[f] for f in fieldnames])
    wb.save(filepath)
    print(f"Wrote {len(rows)} rows -> {filepath}")


if __name__ == "__main__":
    import os
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)

    employees, events = generate_employees_and_events()

    write_employees_excel(employees, os.path.join(out_dir, "employees.xlsx"))

    write_csv(
        DEPARTMENTS, os.path.join(out_dir, "departments.csv"),
        fieldnames=["department_id", "department_name", "cost_center"]
    )
    write_csv(
        JOB_ROLES, os.path.join(out_dir, "job_roles.csv"),
        fieldnames=["job_role_id", "job_title", "job_level"]
    )
    write_csv(
        events, os.path.join(out_dir, "employee_events.csv"),
        fieldnames=["event_id", "employee_id", "event_type", "event_date",
                    "department_id", "job_role_id", "manager_employee_id",
                    "salary", "notes"]
    )

    print("\nDone. Summary:")
    print(f"  Employees: {len(employees)}")
    print(f"  Events: {len(events)}")
    print(f"  Active: {sum(1 for e in employees if e['is_active'] == 'TRUE')}")
    print(f"  Exited: {sum(1 for e in employees if e['is_active'] == 'FALSE')}")
