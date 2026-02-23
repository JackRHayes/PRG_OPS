"""
Generate realistic sample PRG job data for testing.
Run: python generate_sample_data.py
"""
import csv
import random
from datetime import date, timedelta

random.seed(42)

UTILITY_OWNERS = ["Con Edison", "PSEG", "National Grid", "Consolidated Gas"]
CONTRACTORS = [
    "XYZ Underground LLC",
    "Metro Civil Corp",
    "Tri-State Utilities Inc",
    "Empire Excavation",
    "Consolidated Field Svcs",
]
SCOPE_TYPES = ["Main Repair", "Service Install", "Valve Replacement", "Emergency Repair", "Planned Upgrade"]
REGIONS = ["Bronx", "Brooklyn", "Queens", "Manhattan", "Staten Island", "Nassau", "Suffolk"]
CREW_TYPES = ["Civil", "Gas", "Water"]
STATUSES = ["Open", "In Progress", "Completed"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_jobs(n=200):
    jobs = []
    base_start = date(2023, 1, 1)
    base_end = date(2025, 1, 31)

    for i in range(1, n + 1):
        start = random_date(base_start, base_end)
        planned_duration = random.randint(5, 60)
        planned_end = start + timedelta(days=planned_duration)

        # Skew toward completed so ML has enough training data
        status = random.choices(STATUSES, weights=[0.2, 0.25, 0.55])[0]
        contractor = random.choice(CONTRACTORS)

        # Simulate bad contractors having more issues and longer delays
        if contractor in ["XYZ Underground LLC", "Empire Excavation"]:
            markout_issues = random.randint(0, 8)
            inspections_failed = random.randint(0, 5)
            # 40% chance of major delay (30+ days over plan)
            if random.random() < 0.40:
                extra_days = random.randint(30, 90)
            else:
                extra_days = random.randint(0, 20)
        else:
            markout_issues = random.randint(0, 3)
            inspections_failed = random.randint(0, 2)
            # 10% chance of major delay
            if random.random() < 0.10:
                extra_days = random.randint(30, 60)
            else:
                extra_days = random.randint(0, 10)

        actual_end = None
        if status == "Completed":
            actual_days = planned_duration + extra_days
            actual_end = start + timedelta(days=actual_days)

        jobs.append({
            "job_id": f"PRG-2024-{str(i).zfill(4)}",
            "utility_owner": random.choice(UTILITY_OWNERS),
            "contractor": contractor,
            "scope_type": random.choice(SCOPE_TYPES),
            "region": random.choice(REGIONS),
            "start_date": start.isoformat(),
            "planned_end_date": planned_end.isoformat(),
            "actual_end_date": actual_end.isoformat() if actual_end else "",
            "status": status,
            "markout_required": random.choice([True, False]),
            "markout_issues": markout_issues,
            "inspections_failed": inspections_failed,
            "crew_type": random.choice(CREW_TYPES),
        })

    # Inject a few intentionally bad/invalid records for validation testing
    jobs.append({
        "job_id": "PRG-BAD-001",
        "utility_owner": "Con Edison",
        "contractor": "XYZ Underground LLC",
        "scope_type": "Main Repair",
        "region": "Bronx",
        "start_date": "2024-12-01",
        "planned_end_date": "2024-11-01",  # end before start — invalid
        "actual_end_date": "",
        "status": "Open",
        "markout_required": True,
        "markout_issues": -1,  # negative — invalid
        "inspections_failed": 0,
        "crew_type": "Civil",
    })
    jobs.append({
        "job_id": "",  # missing ID — invalid
        "utility_owner": "PSEG",
        "contractor": "Metro Civil Corp",
        "scope_type": "Valve Replacement",
        "region": "Queens",
        "start_date": "not-a-date",  # bad date
        "planned_end_date": "2025-01-15",
        "actual_end_date": "",
        "status": "In Progress",
        "markout_required": False,
        "markout_issues": 0,
        "inspections_failed": 0,
        "crew_type": "Water",
    })

    return jobs

if __name__ == "__main__":
    jobs = generate_jobs(200)
    output_path = "data/sample_jobs.csv"
    fieldnames = [
        "job_id", "utility_owner", "contractor", "scope_type", "region",
        "start_date", "planned_end_date", "actual_end_date", "status",
        "markout_required", "markout_issues", "inspections_failed", "crew_type",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)
    print(f"Sample data written to {output_path} ({len(jobs)} records, 2 invalid injected)")
