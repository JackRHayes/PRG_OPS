"""
MODULE — What-If Scenario Engine
Simulates the impact of changing a job's inputs to see how it affects risk and cost exposure.
"""
import copy
from risk_engine import calculate_job_risk, _get_model
from predictive_model import predict_delay_risk
from explainability import get_job_explanation
from cost_impact import calculate_financial_exposure

def run_scenario(job: dict, contractor_scores: dict, modifications: dict) -> dict:
    """
    Simulates modifications to a job and compares risk/cost against the original.
    """
    model = _get_model()

    # --- Original evaluation ---
    orig_job = copy.deepcopy(job)
    crf = contractor_scores.get(orig_job.get("contractor", ""), {}).get("contractor_risk_factor", 0.0)
    
    orig_score = calculate_job_risk(orig_job, crf)
    orig_ml = predict_delay_risk(orig_job, model, contractor_scores)
    orig_cost = calculate_financial_exposure(orig_ml.get("expected_delay_days"), orig_ml.get("confidence", "low"))
    
    # --- Modified evaluation ---
    mod_job = copy.deepcopy(job)
    for k, v in modifications.items():
        # Handle type conversion automatically based on orig value type if possible
        if k in mod_job and isinstance(mod_job[k], int):
            mod_job[k] = int(v)
        elif k in mod_job and isinstance(mod_job[k], float):
            mod_job[k] = float(v)
        else:
            # Simple digit check fallback
            if str(v).lstrip('-').isdigit(): 
                mod_job[k] = int(v)
            else: 
                try:
                    mod_job[k] = float(v)
                except ValueError:
                    mod_job[k] = v

    mod_crf = contractor_scores.get(mod_job.get("contractor", ""), {}).get("contractor_risk_factor", 0.0)
    
    mod_score = calculate_job_risk(mod_job, mod_crf)
    mod_ml = predict_delay_risk(mod_job, model, contractor_scores)
    mod_cost = calculate_financial_exposure(mod_ml.get("expected_delay_days"), mod_ml.get("confidence", "low"))
    mod_expl = get_job_explanation(mod_job, model, contractor_scores, mod_ml.get("ml_risk_level", "LOW"))

    orig_prob = orig_ml.get("major_delay_probability") or 0.0
    mod_prob  = mod_ml.get("major_delay_probability") or 0.0
    
    orig_days = orig_ml.get("expected_delay_days") or 0.0
    mod_days  = mod_ml.get("expected_delay_days") or 0.0

    return {
        "job_id": job.get("job_id"),
        "modifications": modifications,
        "original": {
            "risk_score": orig_score,
            "ml_delay_probability": orig_prob,
            "ml_expected_delay_days": orig_days,
            "expected_exposure": orig_cost["expected_cost"]
        },
        "modified": {
            "risk_score": mod_score,
            "ml_delay_probability": mod_prob,
            "ml_expected_delay_days": mod_days,
            "expected_exposure": mod_cost["expected_cost"],
            "explanation": mod_expl["explanation_string"]
        },
        "delta": {
            "risk_score_change": round(mod_score - orig_score, 2),
            "ml_delay_probability_change": round(mod_prob - orig_prob, 1),
            "expected_delay_days_change": round(mod_days - orig_days, 1),
            "cost_exposure_change": mod_cost["expected_cost"] - orig_cost["expected_cost"]
        }
    }
