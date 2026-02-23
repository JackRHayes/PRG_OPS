"""
MODULE 3 — Compliance & Issue Analysis
Identifies markout and inspection patterns that lead to delays.
"""
from collections import defaultdict


REPEAT_ISSUE_THRESHOLD = 2  # Jobs with this many or more total issues flagged


def calculate_compliance_metrics(jobs: list[dict]) -> list[dict]:
    """
    Aggregate markout + inspection metrics by contractor.
    Returns list of dicts sorted by total issue count descending.
    """
    buckets = defaultdict(lambda: {
        "jobs": 0,
        "markout_issue_sum": 0,
        "inspections_failed_sum": 0,
        "markout_required_count": 0,
    })

    for job in jobs:
        c = job["contractor"]
        buckets[c]["jobs"] += 1
        buckets[c]["markout_issue_sum"] += job.get("markout_issues", 0)
        buckets[c]["inspections_failed_sum"] += job.get("inspections_failed", 0)
        if job.get("markout_required"):
            buckets[c]["markout_required_count"] += 1

    result = []
    for contractor, data in buckets.items():
        n = data["jobs"]
        total_markout_jobs = data["markout_required_count"] or 1

        result.append({
            "contractor": contractor,
            "job_count": n,
            "total_markout_issues": data["markout_issue_sum"],
            "avg_markout_issues": round(data["markout_issue_sum"] / n, 2),
            "total_inspections_failed": data["inspections_failed_sum"],
            "inspection_fail_rate": round(data["inspections_failed_sum"] / n, 2),
            "markout_issue_rate": round(data["markout_issue_sum"] / total_markout_jobs, 2),
        })

    return sorted(result, key=lambda x: x["total_markout_issues"] + x["total_inspections_failed"], reverse=True)


def identify_repeat_issue_jobs(jobs: list[dict]) -> list[dict]:
    """
    Flags individual jobs with repeated compliance issues.
    A 'repeat issue job' has markout_issues + inspections_failed >= threshold.
    Returns sorted list of high-issue jobs.
    """
    flagged = []
    for job in jobs:
        total_issues = job.get("markout_issues", 0) + job.get("inspections_failed", 0)
        if total_issues >= REPEAT_ISSUE_THRESHOLD:
            flagged.append({
                "job_id": job["job_id"],
                "contractor": job["contractor"],
                "scope_type": job["scope_type"],
                "region": job["region"],
                "status": job["status"],
                "markout_issues": job.get("markout_issues", 0),
                "inspections_failed": job.get("inspections_failed", 0),
                "total_issues": total_issues,
                "delay_days": job.get("delay_days", 0),
            })

    return sorted(flagged, key=lambda x: x["total_issues"], reverse=True)
