"""
MODULE — Permit Tracking & Analysis
Tracks permit status, expiration, and blocked jobs.
"""
import csv
from datetime import date, datetime, timedelta
from collections import defaultdict

EXPIRY_WARNING_DAYS = 14  # Flag permits expiring within this many days


def parse_date(val):
    if not val or str(val).strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            pass
    return None


def load_permits(file_path: str) -> list[dict]:
    permits = []
    with open(file_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            permits.append({
                "permit_id": row.get("permit_id", "").strip(),
                "job_id": row.get("job_id", "").strip(),
                "permit_type": row.get("permit_type", "").strip(),
                "issuing_authority": row.get("issuing_authority", "").strip(),
                "applied_date": parse_date(row.get("applied_date")),
                "approved_date": parse_date(row.get("approved_date")),
                "expiration_date": parse_date(row.get("expiration_date")),
                "status": row.get("status", "").strip(),
                "days_waiting": int(row.get("days_waiting") or 0),
                "blocked_reason": row.get("blocked_reason", "").strip(),
            })
    return permits


def enrich_permits(permits: list[dict]) -> list[dict]:
    """Add computed fields: days_until_expiry, expiring_soon, is_blocking."""
    today = date.today()
    for p in permits:
        # Days until expiry
        if p["expiration_date"]:
            p["days_until_expiry"] = (p["expiration_date"] - today).days
        else:
            p["days_until_expiry"] = None

        # Expiring soon flag
        p["expiring_soon"] = (
            p["days_until_expiry"] is not None and
            0 <= p["days_until_expiry"] <= EXPIRY_WARNING_DAYS
        )

        # Is this permit blocking the job?
        p["is_blocking"] = p["status"] in ("Blocked", "Applied", "Pending", "Expired")

        # Days waiting for pending/applied
        if p["status"] in ("Applied", "Pending") and p["applied_date"]:
            p["days_waiting"] = (today - p["applied_date"]).days

    return permits


def get_permit_summary(permits: list[dict]) -> dict:
    approved = [p for p in permits if p["status"] == "Approved"]
    pending = [p for p in permits if p["status"] in ("Applied", "Pending")]
    blocked = [p for p in permits if p["status"] == "Blocked"]
    expired = [p for p in permits if p["status"] == "Expired"]
    expiring_soon = [p for p in permits if p.get("expiring_soon")]

    avg_wait = (
        round(sum(p["days_waiting"] for p in pending) / len(pending), 1)
        if pending else 0
    )

    return {
        "total": len(permits),
        "approved": len(approved),
        "pending": len(pending),
        "blocked": len(blocked),
        "expired": len(expired),
        "expiring_soon": len(expiring_soon),
        "avg_wait_days": avg_wait,
    }


def get_permits_by_job(permits: list[dict]) -> dict:
    """Returns dict keyed by job_id with permit counts and blocking flags."""
    by_job = defaultdict(lambda: {
        "total": 0, "approved": 0, "pending": 0,
        "blocked": 0, "expired": 0, "expiring_soon": 0,
        "is_blocking": False,
    })
    for p in permits:
        jid = p["job_id"]
        by_job[jid]["total"] += 1
        status = p["status"].lower()
        if status == "approved":
            by_job[jid]["approved"] += 1
        if status in ("applied", "pending"):
            by_job[jid]["pending"] += 1
        if status == "blocked":
            by_job[jid]["blocked"] += 1
        if status == "expired":
            by_job[jid]["expired"] += 1
        if p.get("expiring_soon"):
            by_job[jid]["expiring_soon"] += 1
        if p.get("is_blocking"):
            by_job[jid]["is_blocking"] = True
    return dict(by_job)


def get_blocked_jobs(permits_by_job: dict) -> list[dict]:
    """Returns jobs where at least one permit is blocking progress."""
    blocked = []
    for job_id, data in permits_by_job.items():
        if data["is_blocking"]:
            blocked.append({"job_id": job_id, **data})
    return sorted(blocked, key=lambda x: x["blocked"] + x["expired"], reverse=True)


def get_expiring_permits(permits: list[dict]) -> list[dict]:
    """Returns approved permits expiring within warning window, sorted soonest first."""
    expiring = [p for p in permits if p.get("expiring_soon")]
    return sorted(expiring, key=lambda x: x["days_until_expiry"])


def get_permits_by_type(permits: list[dict]) -> list[dict]:
    buckets = defaultdict(lambda: {"total": 0, "approved": 0, "pending": 0, "blocked": 0})
    for p in permits:
        t = p["permit_type"]
        buckets[t]["total"] += 1
        if p["status"] == "Approved":
            buckets[t]["approved"] += 1
        elif p["status"] in ("Applied", "Pending"):
            buckets[t]["pending"] += 1
        elif p["status"] == "Blocked":
            buckets[t]["blocked"] += 1
    return [{"permit_type": k, **v} for k, v in sorted(buckets.items(), key=lambda x: x[1]["total"], reverse=True)]
