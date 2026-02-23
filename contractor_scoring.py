"""
MODULE 4 — Contractor Performance Scoring
Objectively scores contractor reliability using delay, markout, and inspection data.
"""
from collections import defaultdict


def calculate_contractor_scores(jobs: list[dict]) -> dict[str, dict]:
    """
    Computes a risk score per contractor using:
        contractor_risk = (
            avg_delay_days * 0.4 +
            avg_markout_issues * 0.3 +
            inspection_fail_rate * 0.3
        )
    Returns dict keyed by contractor name.
    """
    buckets = defaultdict(lambda: {
        "jobs": 0,
        "delay_sum": 0,
        "markout_sum": 0,
        "inspections_failed_sum": 0,
    })

    for job in jobs:
        c = job["contractor"]
        buckets[c]["jobs"] += 1
        buckets[c]["delay_sum"] += job.get("delay_days", 0)
        buckets[c]["markout_sum"] += job.get("markout_issues", 0)
        buckets[c]["inspections_failed_sum"] += job.get("inspections_failed", 0)

    scores = {}
    for contractor, data in buckets.items():
        n = data["jobs"]
        avg_delay = data["delay_sum"] / n
        avg_markout = data["markout_sum"] / n
        insp_fail_rate = data["inspections_failed_sum"] / n

        risk_factor = round(
            avg_delay * 0.4 +
            avg_markout * 0.3 +
            insp_fail_rate * 0.3,
            2
        )

        scores[contractor] = {
            "contractor": contractor,
            "job_count": n,
            "avg_delay_days": round(avg_delay, 1),
            "avg_markout_issues": round(avg_markout, 2),
            "inspection_fail_rate": round(insp_fail_rate, 2),
            "contractor_risk_factor": risk_factor,
        }

    return scores


def get_ranked_contractors(contractor_scores: dict) -> list[dict]:
    """Returns contractor list sorted by risk factor descending (worst first)."""
    ranked = sorted(
        contractor_scores.values(),
        key=lambda x: x["contractor_risk_factor"],
        reverse=True,
    )
    for i, c in enumerate(ranked):
        c["rank"] = i + 1
    return ranked


def generate_contractor_report(contractor: str, contractor_scores: dict, jobs: list[dict]) -> str:
    """
    Generates a human-readable text report for a single contractor.
    """
    if contractor not in contractor_scores:
        return f"No data found for contractor: '{contractor}'"

    score = contractor_scores[contractor]
    contractor_jobs = [j for j in jobs if j["contractor"] == contractor]

    open_jobs = [j for j in contractor_jobs if j["status"] in ("Open", "In Progress")]
    completed_jobs = [j for j in contractor_jobs if j["status"] == "Completed"]
    delayed_jobs = [j for j in contractor_jobs if j.get("delay_days", 0) > 0]

    lines = [
        f"{'=' * 55}",
        f"  CONTRACTOR REPORT: {contractor.upper()}",
        f"{'=' * 55}",
        f"  Total Jobs:            {score['job_count']}",
        f"  Open / In Progress:    {len(open_jobs)}",
        f"  Completed:             {len(completed_jobs)}",
        f"  Jobs with Delays:      {len(delayed_jobs)}",
        f"",
        f"  Avg Delay (days):      {score['avg_delay_days']}",
        f"  Avg Markout Issues:    {score['avg_markout_issues']}",
        f"  Inspection Fail Rate:  {score['inspection_fail_rate']}",
        f"",
        f"  >>> RISK FACTOR:       {score['contractor_risk_factor']}",
        f"{'=' * 55}",
    ]

    if open_jobs:
        lines.append("\n  ACTIVE JOBS:")
        for j in open_jobs:
            delay = j.get("delay_days", 0)
            flag = " ⚠" if delay > 0 else ""
            lines.append(
                f"    [{j['job_id']}] {j['scope_type']} | {j['region']} | "
                f"Delay: {delay}d | Markout: {j.get('markout_issues',0)} | "
                f"Insp Fails: {j.get('inspections_failed',0)}{flag}"
            )

    return "\n".join(lines)
