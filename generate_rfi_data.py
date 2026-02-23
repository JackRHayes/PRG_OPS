"""
Generate realistic sample RFI and Submittal data for testing.
Run: python3 generate_rfi_data.py
"""
import csv
import random
from datetime import date, timedelta

random.seed(99)

JOB_IDS = [f"PRG-2024-{str(i).zfill(4)}" for i in range(1, 81)]

RFI_SUBJECTS = [
    "Clarification on pipe depth requirement",
    "Soil condition discrepancy",
    "Utility conflict at station 12+50",
    "Drawing inconsistency - sheet C-4",
    "Markout boundary question",
    "Traffic control plan approval",
    "Material substitution request",
    "Inspection hold point clarification",
    "Trench width specification",
    "Backfill compaction requirements",
]

SUBMITTAL_TYPES = [
    "Traffic Control Plan",
    "Material Data Sheet",
    "Pipe Specifications",
    "Concrete Mix Design",
    "Shoring Plan",
    "Safety Plan",
    "Environmental Compliance",
    "Crew Qualifications",
]

STATUSES_RFI = ["Open", "Answered", "Closed"]
STATUSES_SUB = ["Pending Review", "Approved", "Approved with Comments", "Rejected", "Resubmit Required"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_rfis(n=60):
    rfis = []
    base = date(2024, 7, 1)
    end = date(2025, 1, 31)

    for i in range(1, n + 1):
        job_id = random.choice(JOB_IDS)
        submitted = random_date(base, end)
        status = random.choices(STATUSES_RFI, weights=[0.4, 0.4, 0.2])[0]

        if status == "Open":
            days_open = (date.today() - submitted).days
            answered_date = ""
            response_days = ""
        else:
            response_days = random.randint(2, 30)
            answered_date = (submitted + timedelta(days=response_days)).isoformat()
            days_open = 0

        rfis.append({
            "rfi_id": f"RFI-{str(i).zfill(4)}",
            "job_id": job_id,
            "subject": random.choice(RFI_SUBJECTS),
            "submitted_date": submitted.isoformat(),
            "answered_date": answered_date,
            "status": status,
            "days_open": days_open,
            "response_days": response_days,
            "submitted_by": random.choice(["Field", "Office", "Contractor"]),
        })

    return rfis


def generate_submittals(n=50):
    submittals = []
    base = date(2024, 7, 1)
    end = date(2025, 1, 31)

    for i in range(1, n + 1):
        job_id = random.choice(JOB_IDS)
        submitted = random_date(base, end)
        required_by = submitted + timedelta(days=random.randint(7, 21))
        status = random.choices(STATUSES_SUB, weights=[0.3, 0.3, 0.2, 0.1, 0.1])[0]

        if status in ("Pending Review", "Resubmit Required"):
            reviewed_date = ""
            overdue = date.today() > required_by
        else:
            reviewed_date = (submitted + timedelta(days=random.randint(3, 20))).isoformat()
            overdue = False

        submittals.append({
            "submittal_id": f"SUB-{str(i).zfill(4)}",
            "job_id": job_id,
            "type": random.choice(SUBMITTAL_TYPES),
            "submitted_date": submitted.isoformat(),
            "required_by_date": required_by.isoformat(),
            "reviewed_date": reviewed_date,
            "status": status,
            "overdue": overdue,
            "resubmit_count": random.randint(0, 2) if status == "Resubmit Required" else 0,
        })

    return submittals


if __name__ == "__main__":
    rfis = generate_rfis(60)
    rfi_fields = ["rfi_id", "job_id", "subject", "submitted_date", "answered_date",
                  "status", "days_open", "response_days", "submitted_by"]
    with open("data/sample_rfis.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rfi_fields)
        writer.writeheader()
        writer.writerows(rfis)
    print(f"RFI data written → data/sample_rfis.csv ({len(rfis)} records)")

    submittals = generate_submittals(50)
    sub_fields = ["submittal_id", "job_id", "type", "submitted_date", "required_by_date",
                  "reviewed_date", "status", "overdue", "resubmit_count"]
    with open("data/sample_submittals.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sub_fields)
        writer.writeheader()
        writer.writerows(submittals)
    print(f"Submittal data written → data/sample_submittals.csv ({len(submittals)} records)")
