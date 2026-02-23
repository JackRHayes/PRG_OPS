"""
MODULE 6 — Report Generation & Output Writers
Writes all output artifacts: CSVs, scorecards, and the weekly ops summary.
"""
import csv
import os
from datetime import date


def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def write_high_risk_jobs(scored_jobs: list[dict], output_path: str = "outputs/high_risk_jobs.csv"):
    """Writes HIGH risk jobs to CSV."""
    _ensure_dir(output_path)
    high_risk = [j for j in scored_jobs if j["risk_level"] == "HIGH"]

    fields = [
        "job_id", "utility_owner", "contractor", "scope_type", "region",
        "status", "start_date", "planned_end_date",
        "actual_duration_days", "delay_days", "delay_pct",
        "markout_issues", "inspections_failed",
        "contractor_risk_factor", "risk_score", "risk_level", "risk_reasons",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for job in high_risk:
            row = {k: job[k] for k in fields if k in job}
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
            writer.writerow(row)

    print(f"  High-risk jobs saved → {output_path} ({len(high_risk)} jobs flagged)")
    return high_risk


def write_all_scored_jobs(scored_jobs: list[dict], output_path: str = "outputs/all_scored_jobs.csv"):
    """Writes all active scored jobs (all risk levels) to CSV."""
    _ensure_dir(output_path)
    fields = [
        "job_id", "utility_owner", "contractor", "scope_type", "region",
        "status", "start_date", "planned_end_date",
        "actual_duration_days", "delay_days", "delay_pct",
        "markout_issues", "inspections_failed",
        "contractor_risk_factor", "risk_score", "risk_level", "risk_reasons",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for job in scored_jobs:
            row = {k: job[k] for k in fields if k in job}
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
            writer.writerow(row)
    print(f"  All scored jobs saved → {output_path} ({len(scored_jobs)} active jobs)")


def write_contractor_scorecards(ranked_contractors: list[dict], output_path: str = "outputs/contractor_scorecards.csv"):
    """Writes contractor scorecard CSV."""
    _ensure_dir(output_path)
    fields = [
        "rank", "contractor", "job_count",
        "avg_delay_days", "avg_markout_issues",
        "inspection_fail_rate", "contractor_risk_factor",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ranked_contractors)
    print(f"  Contractor scorecards saved → {output_path} ({len(ranked_contractors)} contractors)")


def write_weekly_summary(
    jobs: list[dict],
    scored_jobs: list[dict],
    ranked_contractors: list[dict],
    compliance_metrics: list[dict],
    output_path: str = "outputs/weekly_ops_summary.txt",
):
    """Writes a human-readable weekly summary for leadership."""
    _ensure_dir(output_path)
    today = date.today()

    total = len(jobs)
    open_jobs = [j for j in jobs if j["status"] in ("Open", "In Progress")]
    completed = [j for j in jobs if j["status"] == "Completed"]
    delayed_completed = [j for j in completed if j.get("delay_days", 0) > 0]

    high_risk = [j for j in scored_jobs if j["risk_level"] == "HIGH"]
    medium_risk = [j for j in scored_jobs if j["risk_level"] == "MEDIUM"]
    low_risk = [j for j in scored_jobs if j["risk_level"] == "LOW"]

    avg_delay = (
        round(sum(j.get("delay_days", 0) for j in completed) / len(completed), 1)
        if completed else 0
    )

    lines = [
        f"{'=' * 60}",
        f"  PRG OPERATIONS — WEEKLY RISK & DELAY SUMMARY",
        f"  Generated: {today.strftime('%B %d, %Y')}",
        f"{'=' * 60}",
        f"",
        f"  PORTFOLIO OVERVIEW",
        f"  {'─' * 40}",
        f"  Total Jobs in System:     {total}",
        f"  Active (Open/In Prog):    {len(open_jobs)}",
        f"  Completed:                {len(completed)}",
        f"  Completed with Delays:    {len(delayed_completed)}",
        f"  Avg Delay (completed):    {avg_delay} days",
        f"",
        f"  ACTIVE JOB RISK BREAKDOWN",
        f"  {'─' * 40}",
        f"  🔴 HIGH Risk:             {len(high_risk)}",
        f"  🟡 MEDIUM Risk:           {len(medium_risk)}",
        f"  🟢 LOW Risk:              {len(low_risk)}",
        f"",
    ]

    if high_risk:
        lines.append(f"  TOP HIGH-RISK JOBS (top 5):")
        for job in high_risk[:5]:
            lines.append(
                f"    [{job['job_id']}] {job['contractor']} | {job['scope_type']} | "
                f"{job['region']} | Score: {job['risk_score']} | {job['risk_reasons']}"
            )
        lines.append("")

    lines += [
        f"  CONTRACTOR RISK RANKING",
        f"  {'─' * 40}",
    ]
    for c in ranked_contractors[:5]:
        lines.append(
            f"  #{c['rank']} {c['contractor']:<30} Risk: {c['contractor_risk_factor']:>5} "
            f"| Avg Delay: {c['avg_delay_days']}d | Markout: {c['avg_markout_issues']}"
        )

    lines += [
        f"",
        f"  TOP COMPLIANCE CONCERNS",
        f"  {'─' * 40}",
    ]
    for c in compliance_metrics[:3]:
        lines.append(
            f"  {c['contractor']:<30} Markout Issues: {c['total_markout_issues']} | "
            f"Insp Fails: {c['total_inspections_failed']}"
        )

    lines += [
        f"",
        f"{'=' * 60}",
        f"  END OF REPORT",
        f"{'=' * 60}",
    ]

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"  Weekly summary saved → {output_path}")
