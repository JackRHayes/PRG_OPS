"""
MODULE — RFI & Submittal Analysis
Tracks open RFIs, response times, overdue submittals, and ties them to jobs.
"""
import csv
from datetime import date, datetime
from collections import defaultdict


def parse_date(val):
    if not val or str(val).strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            pass
    return None


def load_rfis(file_path: str) -> list[dict]:
    rfis = []
    with open(file_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rfis.append({
                "rfi_id": row.get("rfi_id", "").strip(),
                "job_id": row.get("job_id", "").strip(),
                "subject": row.get("subject", "").strip(),
                "submitted_date": parse_date(row.get("submitted_date")),
                "answered_date": parse_date(row.get("answered_date")),
                "status": row.get("status", "").strip(),
                "submitted_by": row.get("submitted_by", "").strip(),
            })
    return rfis


def load_submittals(file_path: str) -> list[dict]:
    submittals = []
    with open(file_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            overdue_val = str(row.get("overdue", "False")).strip().lower()
            submittals.append({
                "submittal_id": row.get("submittal_id", "").strip(),
                "job_id": row.get("job_id", "").strip(),
                "type": row.get("type", "").strip(),
                "submitted_date": parse_date(row.get("submitted_date")),
                "required_by_date": parse_date(row.get("required_by_date")),
                "reviewed_date": parse_date(row.get("reviewed_date")),
                "status": row.get("status", "").strip(),
                "overdue": overdue_val in ("true", "1", "yes"),
                "resubmit_count": int(row.get("resubmit_count", 0) or 0),
            })
    return submittals


def enrich_rfis(rfis: list[dict]) -> list[dict]:
    """Add days_open to each RFI."""
    today = date.today()
    for rfi in rfis:
        if rfi["status"] == "Open" and rfi["submitted_date"]:
            rfi["days_open"] = (today - rfi["submitted_date"]).days
        elif rfi["answered_date"] and rfi["submitted_date"]:
            rfi["days_open"] = (rfi["answered_date"] - rfi["submitted_date"]).days
        else:
            rfi["days_open"] = 0
        rfi["overdue"] = rfi["status"] == "Open" and rfi["days_open"] > 14
    return rfis


def enrich_submittals(submittals: list[dict]) -> list[dict]:
    """Mark overdue submittals."""
    today = date.today()
    for s in submittals:
        if s["status"] in ("Pending Review", "Resubmit Required") and s["required_by_date"]:
            s["overdue"] = today > s["required_by_date"]
            s["days_overdue"] = max(0, (today - s["required_by_date"]).days)
        else:
            s["overdue"] = False
            s["days_overdue"] = 0
    return submittals


def get_rfi_summary(rfis: list[dict]) -> dict:
    open_rfis = [r for r in rfis if r["status"] == "Open"]
    answered = [r for r in rfis if r["status"] == "Answered"]
    overdue = [r for r in rfis if r.get("overdue")]
    avg_response = (
        round(sum(r["days_open"] for r in answered) / len(answered), 1)
        if answered else 0
    )
    return {
        "total": len(rfis),
        "open": len(open_rfis),
        "answered": len(answered),
        "overdue": len(overdue),
        "avg_response_days": avg_response,
    }


def get_submittal_summary(submittals: list[dict]) -> dict:
    pending = [s for s in submittals if s["status"] in ("Pending Review", "Resubmit Required")]
    overdue = [s for s in submittals if s.get("overdue")]
    approved = [s for s in submittals if "Approved" in s["status"]]
    resubmits = [s for s in submittals if s["resubmit_count"] > 0]
    return {
        "total": len(submittals),
        "pending": len(pending),
        "overdue": len(overdue),
        "approved": len(approved),
        "resubmit_count": len(resubmits),
    }


def get_rfi_by_job(rfis: list[dict]) -> dict:
    """Returns dict keyed by job_id with RFI counts and overdue flags."""
    by_job = defaultdict(lambda: {"total": 0, "open": 0, "overdue": 0})
    for rfi in rfis:
        jid = rfi["job_id"]
        by_job[jid]["total"] += 1
        if rfi["status"] == "Open":
            by_job[jid]["open"] += 1
        if rfi.get("overdue"):
            by_job[jid]["overdue"] += 1
    return dict(by_job)


def get_submittals_by_job(submittals: list[dict]) -> dict:
    """Returns dict keyed by job_id with submittal counts."""
    by_job = defaultdict(lambda: {"total": 0, "pending": 0, "overdue": 0, "resubmits": 0})
    for s in submittals:
        jid = s["job_id"]
        by_job[jid]["total"] += 1
        if s["status"] in ("Pending Review", "Resubmit Required"):
            by_job[jid]["pending"] += 1
        if s.get("overdue"):
            by_job[jid]["overdue"] += 1
        if s["resubmit_count"] > 0:
            by_job[jid]["resubmits"] += 1
    return dict(by_job)


def get_high_rfi_jobs(rfi_by_job: dict, threshold: int = 3) -> list[dict]:
    """Jobs with too many open or overdue RFIs."""
    flagged = []
    for job_id, data in rfi_by_job.items():
        if data["open"] >= threshold or data["overdue"] > 0:
            flagged.append({"job_id": job_id, **data})
    return sorted(flagged, key=lambda x: x["open"] + x["overdue"], reverse=True)
