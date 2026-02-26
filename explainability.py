"""
MODULE — Explainability Layer
Identifies the key contributing features driving a job's ML risk prediction
and generates business-friendly explanation strings.
"""

import pandas as pd
from predictive_model import _build_features

def get_job_explanation(job: dict, model_bundle: dict, contractor_scores: dict, ml_risk_level: str) -> dict:
    """
    Returns an explainability dictionary containing:
      - top_features: list of dicts with 'feature' and 'importance'
      - explanation_string: human-readable text
    """
    default_resp = {
        'top_features': [],
        'explanation_string': "Risk determined by standard baseline factors."
    }

    if not model_bundle or not model_bundle.get('classifier'):
        default_resp['explanation_string'] = "Model not trained — explanation unavailable."
        return default_resp

    if ml_risk_level == 'LOW' or not ml_risk_level:
        default_resp['explanation_string'] = "Low risk profile — no major risk indicators present."
        return default_resp

    clf = model_bundle['classifier']
    
    if not hasattr(clf, 'feature_importances_'):
        return default_resp

    importances = clf.feature_importances_

    df = _build_features(pd.DataFrame([job]), contractor_scores)
    
    if df.empty:
        return default_resp
        
    features = df.columns.tolist()
    values = df.iloc[0].tolist()

    planned_days = float(job.get('planned_duration_days', 14.0))

    contributions = []
    for feat, val, imp in zip(features, values, importances):
        is_risk_driver = False

        if feat in ['markout_issues', 'inspections_failed']:
            if val > 0: is_risk_driver = True
        elif feat == 'contractor_risk_factor':
            if val > 0.0: is_risk_driver = True
        elif feat == 'season':
            if val == 3: is_risk_driver = True # Winter
        elif feat == 'planned_duration_days':
            if val > 30: is_risk_driver = True
        elif feat == 'actual_duration_days':
            if val > planned_days and planned_days > 0: is_risk_driver = True
        elif feat.startswith('scope_') or feat.startswith('region_'):
            if val == 1: is_risk_driver = True

        if is_risk_driver:
            contributions.append({
                'feature': feat,
                'importance': imp,
                'value': val
            })

    # Sort by the model's assigned global feature importance
    contributions = sorted(contributions, key=lambda x: x['importance'], reverse=True)
    top_contributors = contributions[:3]

    if not top_contributors:
        return default_resp

    reasons = []
    for c in top_contributors:
        f = c['feature']
        if f == 'markout_issues':
            reasons.append("markout issues")
        elif f == 'inspections_failed':
            reasons.append("failed inspections")
        elif f == 'contractor_risk_factor':
            reasons.append("contractor delay trend")
        elif f == 'season':
            reasons.append("winter start date")
        elif f == 'planned_duration_days':
            reasons.append("extended planned duration")
        elif f == 'actual_duration_days':
            reasons.append("schedule overrun")
        elif f.startswith('scope_'):
            reasons.append(f"{f.replace('scope_', '').replace('_', ' ')} scope")
        elif f.startswith('region_'):
            reasons.append(f"{f.replace('region_', '').title()} region")
        else:
            reasons.append(f.replace('_', ' '))

    reasons_text = ""
    if len(reasons) > 1:
        reasons_text = ", ".join(reasons[:-1]) + f", and {reasons[-1]}"
    else:
        reasons_text = reasons[0]

    prefix = "HIGH RISK due to" if ml_risk_level == 'HIGH' else "Elevated risk due to"
    explanation_string = f"{prefix} {reasons_text}."

    return {
        'top_features': top_contributors,
        'explanation_string': explanation_string
    }
