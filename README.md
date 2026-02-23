# PRG Utility Job Risk & Delay Intelligence Tool

Internal operations tool for identifying high-risk utility construction jobs
before they escalate. Flags delays, compliance issues, and contractor patterns.

---

## Quick Start

```bash
# 1. Generate sample data (first time only)
python generate_sample_data.py

# 2. Run full analysis
python prg_ops.py run-all data/sample_jobs.csv

# 3. View results
outputs/weekly_ops_summary.txt       ← Leadership summary
outputs/high_risk_jobs.csv           ← Jobs needing attention now
outputs/contractor_scorecards.csv    ← Contractor accountability
outputs/all_scored_jobs.csv          ← All active jobs scored
logs/validation_errors.log           ← Data quality issues
```

---

## CLI Commands

| Command | What it does |
|---|---|
| `python prg_ops.py ingest jobs.csv` | Load & validate job data, run full analysis |
| `python prg_ops.py run-all jobs.csv` | Same as ingest |
| `python prg_ops.py high-risk` | Print HIGH risk jobs to terminal |
| `python prg_ops.py contractor-report "XYZ Underground LLC"` | Single contractor scorecard |
| `python prg_ops.py summary` | Print ops summary to terminal |

---

## Input Format

CSV with these columns:

| Field | Type | Notes |
|---|---|---|
| job_id | string | Unique ID |
| utility_owner | string | Con Ed, PSEG, etc. |
| contractor | string | Performing contractor |
| scope_type | string | Main Repair, Service Install, etc. |
| region | string | Area / division |
| start_date | date | YYYY-MM-DD |
| planned_end_date | date | YYYY-MM-DD |
| actual_end_date | date or blank | YYYY-MM-DD or empty |
| status | string | Open / In Progress / Completed |
| markout_required | bool | True / False |
| markout_issues | int | Count ≥ 0 |
| inspections_failed | int | Count ≥ 0 |
| crew_type | string | Civil / Gas / Water |

---

## Risk Score Formula

```
risk_score = (
    days_open           * 0.35 +
    markout_issues      * 0.25 +
    inspections_failed  * 0.20 +
    contractor_risk     * 0.20
)
```

Risk levels use **dynamic percentile thresholds** based on the active job population:
- 🔴 **HIGH** — Top third of scores
- 🟡 **MEDIUM** — Middle third
- 🟢 **LOW** — Bottom third

## Contractor Risk Formula

```
contractor_risk = (
    avg_delay_days      * 0.4 +
    avg_markout_issues  * 0.3 +
    inspection_fail_rate * 0.3
)
```

---

## Project Structure

```
prg_ops/
├── prg_ops.py              ← CLI entrypoint
├── ingestion.py            ← Module 1: Load & validate data
├── schedule_analysis.py    ← Module 2: Delay calculations
├── compliance_analysis.py  ← Module 3: Markout & inspection patterns
├── contractor_scoring.py   ← Module 4: Contractor performance
├── risk_engine.py          ← Module 5: Job risk scoring
├── reports.py              ← Module 6: Output writers
├── generate_sample_data.py ← Test data generator
├── data/
│   └── sample_jobs.csv
├── outputs/
│   ├── clean_jobs.csv
│   ├── high_risk_jobs.csv
│   ├── all_scored_jobs.csv
│   ├── contractor_scorecards.csv
│   └── weekly_ops_summary.txt
└── logs/
    └── validation_errors.log
```

---

## Requirements

- Python 3.11+
- `openpyxl` (for XLSX input): `pip install openpyxl`
