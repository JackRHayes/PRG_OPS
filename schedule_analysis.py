"""
MODULE 2 — Schedule & Delay Analysis
Quantifies how late jobs are and aggregates delay metrics.
"""
from datetime import date
from collections import defaultdict


def calculate_job_duration(job: dict) -> dict:
    """
    Adds planned_duration_days and actual_duration_days to job dict.
    Returns updated job. Skips calculation if date fields are absent
    (e.g. pre-processed CSV exports that already contain these values).
    """
    start = job.get("start_date")
    planned_end = job.get("planned_end_date")

    if start is None or planned_end is None:
        # Keep pre-computed values if present, otherwise default to 0
        job.setdefault("planned_duration_days", 0)
        job.setdefault("actual_duration_days", int(job.get("actual_duration_days", 0)))
        return job

    today = date.today()
    actual_end = job.get("actual_end_date")
    status = job["status"]

    planned_duration = (planned_end - start).days

    if status == "Completed" and actual_end:
        actual_duration = (actual_end - start).days
    else:
        # Open or In Progress — measure days open so far
        actual_duration = (today - start).days

    job["planned_duration_days"] = max(0, planned_duration)
    job["actual_duration_days"] = max(0, actual_duration)
    return job


def calculate_delay(job: dict) -> dict:
    """
    Adds delay_days and delay_pct to job.
    For completed jobs: actual vs planned.
    For open/in-progress: days past planned end (if any).
    Skips calculation if planned_end_date is absent.
    """
    planned_end = job.get("planned_end_date")

    if planned_end is None:
        # Keep pre-computed values if present, otherwise default to 0
        job.setdefault("delay_days", int(job.get("delay_days", 0)))
        job.setdefault("delay_pct", 0.0)
        return job

    today = date.today()
    actual_end = job.get("actual_end_date")
    status = job["status"]
    planned_duration = job.get("planned_duration_days", 1)

    if status == "Completed" and actual_end:
        delay_days = max(0, (actual_end - planned_end).days)
    else:
        # If today is past planned end, job is running late
        delay_days = max(0, (today - planned_end).days)

    delay_pct = round((delay_days / planned_duration * 100), 1) if planned_duration > 0 else 0.0

    job["delay_days"] = delay_days
    job["delay_pct"] = delay_pct
    return job


def enrich_jobs_with_schedule(jobs: list[dict]) -> list[dict]:
    """Apply duration + delay calculations to all jobs. Returns enriched list."""
    enriched = []
    for job in jobs:
        job = calculate_job_duration(job)
        job = calculate_delay(job)
        enriched.append(job)
    return enriched


def aggregate_delays(jobs: list[dict]) -> dict:
    """
    Aggregates delay metrics by contractor, scope_type, and region.
    Returns dict with keys: 'by_contractor', 'by_scope', 'by_region'
    Each value is a list of dicts with: name, job_count, avg_delay_days, avg_delay_pct, total_delay_days
    """
    def build_agg(group_key: str) -> list[dict]:
        buckets = defaultdict(lambda: {"jobs": 0, "delay_sum": 0, "delay_pct_sum": 0})
        for job in jobs:
            key = job.get(group_key, "Unknown")
            buckets[key]["jobs"] += 1
            buckets[key]["delay_sum"] += job.get("delay_days", 0)
            buckets[key]["delay_pct_sum"] += job.get("delay_pct", 0)

        result = []
        for name, data in sorted(buckets.items()):
            n = data["jobs"]
            result.append({
                group_key: name,
                "job_count": n,
                "avg_delay_days": round(data["delay_sum"] / n, 1),
                "avg_delay_pct": round(data["delay_pct_sum"] / n, 1),
                "total_delay_days": data["delay_sum"],
            })
        return sorted(result, key=lambda x: x["avg_delay_days"], reverse=True)

    return {
        "by_contractor": build_agg("contractor"),
        "by_scope": build_agg("scope_type"),
        "by_region": build_agg("region"),
    }
