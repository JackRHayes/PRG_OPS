"""
MODULE — Cost Impact Layer
Translates predicted delay days into dollar-value financial exposure
using configurable benchmarks.
"""
import os
import json

COST_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'cost_config.json')

DEFAULT_COST_CONFIG = {
    "cost_per_delay_day": 4000,
    "penalty_threshold_days": 30,
    "penalty_flat_fee": 50000
}

def get_cost_config() -> dict:
    if os.path.exists(COST_CONFIG_PATH):
        try:
            with open(COST_CONFIG_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_COST_CONFIG

def calculate_financial_exposure(expected_delay_days: float, confidence: str) -> dict:
    """
    Calculate the estimated cost exposure for a job.
    Returns:
     - expected_cost
     - worst_case_cost
     - confidence_weighted_cost
     - summary_string
    """
    if expected_delay_days is None or expected_delay_days <= 0:
        return {
            "expected_cost": 0.0,
            "worst_case_cost": 0.0,
            "confidence_weighted_cost": 0.0,
            "summary_string": "No material delay exposure"
        }

    config = get_cost_config()
    cost_per_day = config.get('cost_per_delay_day', 4000)
    threshold = config.get('penalty_threshold_days', 30)
    penalty = config.get('penalty_flat_fee', 50000)

    # 1) Expected base cost
    base_cost = expected_delay_days * cost_per_day
    expected_cost = base_cost + (penalty if expected_delay_days >= threshold else 0)

    # 2) Worst-case scenario (Assume 50% worse delay)
    worst_delay = expected_delay_days * 1.5
    worst_case_cost = (worst_delay * cost_per_day) + (penalty if worst_delay >= threshold else 0)

    # 3) Confidence adjustments
    # If confidence is low, the potential exposure variability is higher.
    weight = 1.0
    if confidence == 'medium': weight = 1.2
    if confidence == 'low': weight = 1.5
    
    confidence_weighted_cost = expected_cost * weight

    summary = f"${expected_cost:,.0f} (Expected) | ${worst_case_cost:,.0f} (Worst-case)"

    return {
        "expected_cost": expected_cost,
        "worst_case_cost": worst_case_cost,
        "confidence_weighted_cost": confidence_weighted_cost,
        "summary_string": summary
    }
