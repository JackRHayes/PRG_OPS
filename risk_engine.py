"""
MODULE 5 — Job Risk Scoring Engine (CORE VALUE)
Flags which active jobs need attention now.
"""
from predictive_model import load_model, predict_delay_risk, DEFAULT_MODEL_PATH

_model_bundle = None

def _get_model():
    global _model_bundle
    if _model_bundle is None:
        _model_bundle = load_model(DEFAULT_MODEL_PATH)
    return _model_bundle

# Risk level thresholds — override with percentile-based classification in score_active_jobs
RISK_LOW_MAX = 30
RISK_HIGH_MIN = 60


def dynamic_thresholds(scores: list[float]) -> tuple[float, float]:
    """
    Computes LOW/HIGH thresholds dynamically based on data distribution.
    Bottom 33% = LOW, Top 33% = HIGH, Middle = MEDIUM.
    Falls back to static thresholds if fewer than 3 jobs.
    """
    if len(scores) < 3:
        return RISK_LOW_MAX, RISK_HIGH_MIN
    s = sorted(scores)
    n = len(s)
    low_thresh = s[n // 3]
    high_thresh = s[(2 * n) // 3]
    return low_thresh, high_thresh


def calculate_job_risk(job: dict, contractor_risk_factor: float) -> float:
    """
    Calculates a composite risk score for an active job.

    Formula:
        risk_score = (
            days_open * 0.35 +
            markout_issues * 0.25 +
            inspections_failed * 0.20 +
            contractor_risk_factor * 0.20
        )
    """
    days_open = job.get("actual_duration_days", 0)
    markout_issues = job.get("markout_issues", 0)
    inspections_failed = job.get("inspections_failed", 0)

    score = (
        days_open * 0.35 +
        markout_issues * 0.25 +
        inspections_failed * 0.20 +
        contractor_risk_factor * 0.20
    )
    return round(score, 2)


def classify_risk(score: float) -> str:
    """Maps a numeric score to LOW / MEDIUM / HIGH."""
    if score < RISK_LOW_MAX:
        return "LOW"
    elif score <= RISK_HIGH_MIN:
        return "MEDIUM"
    else:
        return "HIGH"


def build_risk_reasons(job: dict, score: float, risk_level: str) -> list[str]:
    """
    Generates human-readable reasons for why a job is flagged.
    """
    reasons = []
    days_open = job.get("actual_duration_days", 0)
    delay = job.get("delay_days", 0)
    markout = job.get("markout_issues", 0)
    failed = job.get("inspections_failed", 0)

    if days_open > 30:
        reasons.append(f"Job open {days_open} days")
    if delay > 0:
        reasons.append(f"Running {delay} days past planned end")
    if markout >= 3:
        reasons.append(f"High markout issue count ({markout})")
    elif markout > 0:
        reasons.append(f"Markout issues logged ({markout})")
    if failed >= 2:
        reasons.append(f"Multiple inspection failures ({failed})")
    elif failed > 0:
        reasons.append(f"Inspection failure on record ({failed})")
    if not reasons:
        reasons.append("Elevated contractor risk factor")

    return reasons


def score_active_jobs(jobs: list[dict], contractor_scores: dict) -> list[dict]:
    """
    Scores all non-completed jobs and returns them enriched with risk data.
    Uses dynamic percentile-based thresholds for realistic classification.
    Sorted by risk_score descending.
    """
    active_jobs = [j for j in jobs if j["status"] in ("Open", "In Progress")]
    if not active_jobs:
        return []

    # First pass: compute raw scores
    raw = []
    for job in active_jobs:
        c_score = contractor_scores.get(job["contractor"], {})
        contractor_risk = c_score.get("contractor_risk_factor", 0.0)
        score = calculate_job_risk(job, contractor_risk)
        raw.append((job, score, contractor_risk))

    # Compute dynamic thresholds from actual score distribution
    all_scores = [s for _, s, _ in raw]
    low_thresh, high_thresh = dynamic_thresholds(all_scores)

    # Second pass: classify using dynamic thresholds
    model = _get_model()
    scored = []
    for job, score, contractor_risk in raw:
        if score >= high_thresh:
            level = "HIGH"
        elif score >= low_thresh:
            level = "MEDIUM"
        else:
            level = "LOW"
        reasons = build_risk_reasons(job, score, level)

        ml = predict_delay_risk(job, model, contractor_scores)

        scored.append({
            **job,
            "risk_score": score,
            "risk_level": level,
            "risk_reasons": "; ".join(reasons),
            "contractor_risk_factor": round(contractor_risk, 2),
            "ml_delay_probability": ml.get("major_delay_probability"),
            "ml_expected_delay_days": ml.get("expected_delay_days"),
            "ml_risk_level": ml.get("ml_risk_level"),
            "ml_confidence": ml.get("confidence"),
        })

    return sorted(scored, key=lambda x: x["risk_score"], reverse=True)


def get_high_risk_jobs(scored_jobs: list[dict]) -> list[dict]:
    """Returns only HIGH risk jobs."""
    return [j for j in scored_jobs if j["risk_level"] == "HIGH"]
