#!/usr/bin/env python3
"""
PRG Utility Job Risk & Delay Intelligence Tool
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Usage:
  python prg_ops.py ingest <file.csv>          Load & validate job data
  python prg_ops.py high-risk                  Show jobs needing attention now
  python prg_ops.py contractor-report <name>   Scorecard for one contractor
  python prg_ops.py summary                    Full ops summary
  python prg_ops.py run-all <file.csv>         Ingest + full report in one shot
"""

import sys
import os
import pickle

# ── Module imports ────────────────────────────────────────────────────────────
from ingestion import run_ingestion, load_jobs, validate_jobs, save_clean_jobs, log_validation_errors, setup_logging
from schedule_analysis import enrich_jobs_with_schedule, aggregate_delays
from compliance_analysis import calculate_compliance_metrics, identify_repeat_issue_jobs
from contractor_scoring import calculate_contractor_scores, get_ranked_contractors, generate_contractor_report
from risk_engine import score_active_jobs, get_high_risk_jobs
from reports import (
    write_high_risk_jobs,
    write_all_scored_jobs,
    write_contractor_scorecards,
    write_weekly_summary,
)


CLEAN_JOBS_PATH = "outputs/clean_jobs.csv"
CACHE_PATH = "outputs/.cache.pkl"
PIPELINE_CACHE: dict = {}


# ── Core Pipeline ─────────────────────────────────────────────────────────────

def run_full_pipeline(file_path: str) -> dict:
    """Runs all modules and returns all computed data."""
    print("\n" + "━" * 55)
    print("  PRG OPS — RUNNING FULL ANALYSIS PIPELINE")
    print("━" * 55)

    # Module 1: Ingest
    clean_jobs = run_ingestion(file_path)
    if not clean_jobs:
        print("\n[ERROR] No clean jobs after validation. Aborting.")
        sys.exit(1)

    # Module 2: Schedule & Delay
    print("\n[SCHEDULE] Calculating durations and delays...")
    jobs = enrich_jobs_with_schedule(clean_jobs)
    delay_aggs = aggregate_delays(jobs)
    print(f"  Done. {len(jobs)} jobs enriched.")

    # Module 3: Compliance
    print("\n[COMPLIANCE] Analyzing markout & inspection issues...")
    compliance = calculate_compliance_metrics(jobs)
    repeat_issues = identify_repeat_issue_jobs(jobs)
    print(f"  {len(repeat_issues)} jobs with repeat compliance issues found.")

    # Module 4: Contractor Scoring
    print("\n[CONTRACTORS] Scoring contractor performance...")
    contractor_scores = calculate_contractor_scores(jobs)
    ranked = get_ranked_contractors(contractor_scores)
    print(f"  {len(ranked)} contractors scored.")

    # Module 5: Job Risk Scoring
    print("\n[RISK ENGINE] Scoring active jobs...")
    scored_jobs = score_active_jobs(jobs, contractor_scores)
    high_risk = get_high_risk_jobs(scored_jobs)
    print(f"  {len(scored_jobs)} active jobs scored. {len(high_risk)} HIGH risk.")

    # Module 6: Write Outputs
    print("\n[OUTPUTS] Writing reports...")
    write_high_risk_jobs(scored_jobs)
    write_all_scored_jobs(scored_jobs)
    write_contractor_scorecards(ranked)
    write_weekly_summary(jobs, scored_jobs, ranked, compliance)

    result = {
        "jobs": jobs,
        "scored_jobs": scored_jobs,
        "contractor_scores": contractor_scores,
        "ranked_contractors": ranked,
        "compliance": compliance,
        "repeat_issues": repeat_issues,
        "delay_aggs": delay_aggs,
    }

    # Save to disk so other commands can load it
    with open(CACHE_PATH, "wb") as f:
        pickle.dump(result, f)

    PIPELINE_CACHE.update(result)

    print("\n" + "━" * 55)
    print("  PIPELINE COMPLETE")
    print("━" * 55)

    return result


def load_cached_or_abort() -> dict:
    """Loads last pipeline results from disk, or tells user to run ingest first."""
    if PIPELINE_CACHE.get("jobs"):
        return PIPELINE_CACHE
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "rb") as f:
            PIPELINE_CACHE.update(pickle.load(f))
        return PIPELINE_CACHE
    print("\n[ERROR] No job data loaded. Run: python3 prg_ops.py run-all data/sample_jobs.csv first.")
    sys.exit(1)


# ── CLI Commands ──────────────────────────────────────────────────────────────

def cmd_ingest(args):
    if len(args) < 1:
        print("Usage: python prg_ops.py ingest <jobs_file.csv>")
        sys.exit(1)
    file_path = args[0]
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)
    run_full_pipeline(file_path)


def cmd_high_risk(args):
    data = load_cached_or_abort()
    scored = data["scored_jobs"]
    high_risk = get_high_risk_jobs(scored)

    print(f"\n{'━' * 60}")
    print(f"  HIGH RISK JOBS ({len(high_risk)} flagged)")
    print(f"{'━' * 60}")

    if not high_risk:
        print("  ✅ No HIGH risk jobs at this time.")
        return

    for job in high_risk:
        print(f"\n  Job:        {job['job_id']}")
        print(f"  Contractor: {job['contractor']}")
        print(f"  Scope:      {job['scope_type']} | Region: {job['region']}")
        print(f"  Status:     {job['status']} | Days Open: {job.get('actual_duration_days', 0)}")
        print(f"  Risk Score: {job['risk_score']} 🔴 HIGH")
        print(f"  Reasons:    {job['risk_reasons']}")
        print(f"  {'─' * 50}")

    print(f"\n  Full list → outputs/high_risk_jobs.csv")


def cmd_contractor_report(args):
    if len(args) < 1:
        print('Usage: python prg_ops.py contractor-report "Contractor Name"')
        sys.exit(1)
    contractor_name = args[0]
    data = load_cached_or_abort()
    report = generate_contractor_report(
        contractor_name,
        data["contractor_scores"],
        data["jobs"]
    )
    print(f"\n{report}")


def cmd_summary(args):
    data = load_cached_or_abort()
    jobs = data["jobs"]
    scored = data["scored_jobs"]
    ranked = data["ranked_contractors"]

    high = [j for j in scored if j["risk_level"] == "HIGH"]
    med = [j for j in scored if j["risk_level"] == "MEDIUM"]
    low = [j for j in scored if j["risk_level"] == "LOW"]
    completed = [j for j in jobs if j["status"] == "Completed"]
    avg_delay = (
        round(sum(j.get("delay_days", 0) for j in completed) / len(completed), 1)
        if completed else 0
    )

    print(f"\n{'━' * 55}")
    print(f"  PRG OPS SUMMARY")
    print(f"{'━' * 55}")
    print(f"  Total Jobs:           {len(jobs)}")
    print(f"  Active Jobs:          {len(scored)}")
    print(f"  Completed:            {len(completed)}")
    print(f"  Avg Delay:            {avg_delay} days")
    print(f"")
    print(f"  Risk Breakdown:")
    print(f"    🔴 HIGH:            {len(high)}")
    print(f"    🟡 MEDIUM:          {len(med)}")
    print(f"    🟢 LOW:             {len(low)}")
    print(f"")
    print(f"  Riskiest Contractors (Top 3):")
    for c in ranked[:3]:
        print(f"    #{c['rank']} {c['contractor']:<28} Risk Factor: {c['contractor_risk_factor']}")
    print(f"{'━' * 55}")
    print(f"  Full report → outputs/weekly_ops_summary.txt")


def cmd_run_all(args):
    """Convenience: ingest + full report in one shot."""
    cmd_ingest(args)


# ── Dispatch ──────────────────────────────────────────────────────────────────

COMMANDS = {
    "ingest": cmd_ingest,
    "high-risk": cmd_high_risk,
    "contractor-report": cmd_contractor_report,
    "summary": cmd_summary,
    "run-all": cmd_run_all,
}


def print_help():
    print(__doc__)


def main():
    if len(sys.argv) < 2:
        print_help()
        sys.exit(0)

    command = sys.argv[1].lower()
    args = sys.argv[2:]

    if command in ("help", "--help", "-h"):
        print_help()
        sys.exit(0)

    if command not in COMMANDS:
        print(f"\n[ERROR] Unknown command: '{command}'")
        print_help()
        sys.exit(1)

    COMMANDS[command](args)


if __name__ == "__main__":
    main()
