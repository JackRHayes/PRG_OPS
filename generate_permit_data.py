"""
Generate realistic sample permit data for testing.
Run: python3 generate_permit_data.py
"""
import csv
import random
from datetime import date, timedelta

random.seed(77)

JOB_IDS = [f"PRG-2024-{str(i).zfill(4)}" for i in range(1, 201)]

PERMIT_TYPES = [
    "Excavation",
    "Traffic Control",
    "Street Opening",
    "Utility Work",
    "Environmental",
    "Noise Variance",
]

STATUSES = ["Applied", "Pending", "Approved", "Expired", "Blocked"]

ISSUING_AUTHORITIES = [
    "NYC DOT",
    "NYC DEP",
    "Con Edison ROW",
    "PSEG Right of Way",
    "Nassau County DPW",
    "Suffolk County DPW",
]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_permits(n=65):
    permits = []
    base = date(2024, 6, 1)
    today = date.today()

    for i in range(1, n + 1):
        job_id = random.choice(JOB_IDS)
        permit_type = random.choice(PERMIT_TYPES)
        applied_date = random_date(base, today - timedelta(days=10))
        status = random.choices(STATUSES, weights=[0.15, 0.25, 0.4, 0.1, 0.1])[0]

        if status == "Approved":
            approved_date = applied_date + timedelta(days=random.randint(3, 21))
            duration_days = random.choice([30, 60, 90, 180])
            expiration_date = approved_date + timedelta(days=duration_days)
            days_until_expiry = (expiration_date - today).days
            days_waiting = 0
            blocked_reason = ""
        elif status == "Expired":
            approved_date = applied_date + timedelta(days=random.randint(3, 21))
            expiration_date = today - timedelta(days=random.randint(1, 30))
            days_until_expiry = (expiration_date - today).days
            days_waiting = 0
            blocked_reason = ""
        elif status == "Blocked":
            approved_date = ""
            expiration_date = ""
            days_until_expiry = ""
            days_waiting = random.randint(10, 60)
            blocked_reason = random.choice([
                "Missing site plan",
                "Awaiting utility clearance",
                "Community board review",
                "Environmental review pending",
                "Fee payment outstanding",
            ])
        else:  # Applied or Pending
            approved_date = ""
            expiration_date = ""
            days_until_expiry = ""
            days_waiting = (today - applied_date).days
            blocked_reason = ""

        permits.append({
            "permit_id": f"PRM-{str(i).zfill(4)}",
            "job_id": job_id,
            "permit_type": permit_type,
            "issuing_authority": random.choice(ISSUING_AUTHORITIES),
            "applied_date": applied_date.isoformat(),
            "approved_date": approved_date.isoformat() if approved_date else "",
            "expiration_date": expiration_date.isoformat() if expiration_date else "",
            "status": status,
            "days_waiting": days_waiting,
            "days_until_expiry": days_until_expiry,
            "blocked_reason": blocked_reason,
        })

    return permits

if __name__ == "__main__":
    permits = generate_permits(65)
    fields = [
        "permit_id", "job_id", "permit_type", "issuing_authority",
        "applied_date", "approved_date", "expiration_date",
        "status", "days_waiting", "days_until_expiry", "blocked_reason",
    ]
    with open("data/sample_permits.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(permits)
    print(f"Permit data written → data/sample_permits.csv ({len(permits)} records)")
