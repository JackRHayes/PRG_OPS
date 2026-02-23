"""
MODULE — Predictive ML Model for Job Delay Forecasting
Trains Random Forest models on completed jobs to predict delay risk.
"""
import os
import pickle
import logging
from datetime import date
from typing import Optional

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(__file__), 'models')
DEFAULT_MODEL_PATH = os.path.join(MODEL_DIR, 'delay_predictor.pkl')

MAJOR_DELAY_THRESHOLD = 30  # days — binary classification target

SCOPE_TYPES = [
    'Gas Main Replacement', 'Fiber Install', 'Water Line Repair',
    'Road Restoration', 'Electric Underground', 'Sewer Repair',
    'Planned Upgrade', 'Service Install', 'Main Repair',
]

REGIONS = [
    'Northeast', 'Southeast', 'Northwest', 'Southwest',
    'Bronx', 'Manhattan', 'Staten Island', 'Brooklyn', 'Queens',
]


def _get_season(start_date) -> int:
    """Convert start date to season integer: 0=spring, 1=summer, 2=fall, 3=winter."""
    if start_date is None:
        return 3  # default to winter (worst case)
    try:
        month = start_date.month if hasattr(start_date, 'month') else pd.Timestamp(start_date).month
        if month in (3, 4, 5):
            return 0
        elif month in (6, 7, 8):
            return 1
        elif month in (9, 10, 11):
            return 2
        else:
            return 3
    except Exception:
        return 3


def _build_features(jobs_df: pd.DataFrame, contractor_risk_map: dict) -> pd.DataFrame:
    """
    Build feature matrix from a DataFrame of job dicts.
    Handles missing values and encodes categoricals.
    """
    df = jobs_df.copy()

    # Numeric features — fill missing with 0
    df['markout_issues'] = pd.to_numeric(df.get('markout_issues', 0), errors='coerce').fillna(0)
    df['inspections_failed'] = pd.to_numeric(df.get('inspections_failed', 0), errors='coerce').fillna(0)
    df['planned_duration_days'] = pd.to_numeric(df.get('planned_duration_days', 14), errors='coerce').fillna(14)
    df['actual_duration_days'] = pd.to_numeric(df.get('actual_duration_days', 0), errors='coerce').fillna(0)

    # Contractor risk factor from scoring module output
    df['contractor_risk_factor'] = df['contractor'].map(
        lambda c: contractor_risk_map.get(c, {}).get('contractor_risk_factor', 0.0)
        if isinstance(contractor_risk_map.get(c), dict)
        else float(contractor_risk_map.get(c, 0.0))
    ).fillna(0.0)

    # Season from start_date
    df['season'] = df['start_date'].apply(_get_season)

    # One-hot encode scope_type
    for scope in SCOPE_TYPES:
        col = 'scope_' + scope.lower().replace(' ', '_')
        df[col] = (df.get('scope_type', '') == scope).astype(int)

    # One-hot encode region
    for region in REGIONS:
        col = 'region_' + region.lower()
        df[col] = (df.get('region', '') == region).astype(int)

    feature_cols = (
        ['markout_issues', 'inspections_failed', 'planned_duration_days',
         'actual_duration_days', 'contractor_risk_factor', 'season']
        + ['scope_' + s.lower().replace(' ', '_') for s in SCOPE_TYPES]
        + ['region_' + r.lower() for r in REGIONS]
    )

    return df[feature_cols].astype(float)


def train_model(jobs: list, contractor_scores: dict) -> Optional[dict]:
    """
    Train RandomForestClassifier (major delay binary) and
    RandomForestRegressor (delay days continuous) on completed jobs.

    Args:
        jobs: list of enriched job dicts (must have delay_days)
        contractor_scores: output of calculate_contractor_scores()

    Returns:
        model bundle dict, or None if insufficient data.
    """
    completed = [j for j in jobs if j.get('status') == 'Completed']
    if len(completed) < 10:
        logger.warning(f"[ML] Only {len(completed)} completed jobs — need at least 10 to train.")
        return None

    df = pd.DataFrame(completed)
    X = _build_features(df, contractor_scores)
    y_reg = df['delay_days'].fillna(0).clip(lower=0).astype(float)
    y_cls = (y_reg >= MAJOR_DELAY_THRESHOLD).astype(int)

    # Need at least both classes present for classifier
    if y_cls.nunique() < 2:
        logger.warning("[ML] All completed jobs have same delay class — skipping classifier training.")
        clf = None
        clf_metrics = {}
    else:
        X_tr, X_te, yc_tr, yc_te = train_test_split(X, y_cls, test_size=0.2, random_state=42)
        clf = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, class_weight='balanced')
        clf.fit(X_tr, yc_tr)
        yc_pred = clf.predict(X_te)
        clf_metrics = {
            'accuracy':  round(accuracy_score(yc_te, yc_pred), 3),
            'precision': round(precision_score(yc_te, yc_pred, zero_division=0), 3),
            'recall':    round(recall_score(yc_te, yc_pred, zero_division=0), 3),
            'train_samples': len(X_tr),
            'test_samples':  len(X_te),
        }
        logger.info(f"[ML] Classifier — acc={clf_metrics['accuracy']} prec={clf_metrics['precision']} rec={clf_metrics['recall']}")

    X_tr, X_te, yr_tr, yr_te = train_test_split(X, y_reg, test_size=0.2, random_state=42)
    reg = RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42)
    reg.fit(X_tr, yr_tr)
    yr_pred = reg.predict(X_te)
    reg_metrics = {
        'mae': round(mean_absolute_error(yr_te, yr_pred), 2),
        'r2':  round(r2_score(yr_te, yr_pred), 3),
        'train_samples': len(X_tr),
        'test_samples':  len(X_te),
    }
    logger.info(f"[ML] Regressor — MAE={reg_metrics['mae']} R²={reg_metrics['r2']}")

    bundle = {
        'classifier':        clf,
        'regressor':         reg,
        'clf_metrics':       clf_metrics,
        'reg_metrics':       reg_metrics,
        'trained_on':        len(completed),
        'trained_at':        date.today().isoformat(),
        'major_delay_days':  MAJOR_DELAY_THRESHOLD,
        'scope_types':       SCOPE_TYPES,
        'regions':           REGIONS,
    }
    return bundle


def predict_delay_risk(job: dict, model_bundle: dict, contractor_scores: dict) -> dict:
    """
    Predict delay risk for a single active job.

    Returns:
        {
          'major_delay_probability': 0-100 float,
          'expected_delay_days': float,
          'ml_risk_level': 'LOW' | 'MEDIUM' | 'HIGH',
          'confidence': 'low' | 'medium' | 'high'
        }
    """
    if model_bundle is None:
        return {
            'major_delay_probability': None,
            'expected_delay_days': None,
            'ml_risk_level': None,
            'confidence': 'none',
        }

    df = pd.DataFrame([job])
    X = _build_features(df, contractor_scores)

    # Regression — expected delay days
    expected_delay = max(0.0, round(float(model_bundle['regressor'].predict(X)[0]), 1))

    # Classification — probability of major delay
    clf = model_bundle.get('classifier')
    if clf is not None:
        proba = clf.predict_proba(X)[0]
        classes = list(clf.classes_)
        major_prob = float(proba[classes.index(1)]) if 1 in classes else 0.0
    else:
        # Fallback: derive from regressor output
        major_prob = min(1.0, expected_delay / (MAJOR_DELAY_THRESHOLD * 2))

    major_prob_pct = round(major_prob * 100, 1)

    # ML risk level
    if major_prob_pct >= 60:
        ml_level = 'HIGH'
    elif major_prob_pct >= 30:
        ml_level = 'MEDIUM'
    else:
        ml_level = 'LOW'

    # Confidence based on training size
    n = model_bundle.get('trained_on', 0)
    confidence = 'high' if n >= 50 else ('medium' if n >= 20 else 'low')

    return {
        'major_delay_probability': major_prob_pct,
        'expected_delay_days':     expected_delay,
        'ml_risk_level':           ml_level,
        'confidence':              confidence,
    }


def save_model(model_bundle: dict, filepath: str = DEFAULT_MODEL_PATH) -> bool:
    """Persist trained model bundle to disk."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(model_bundle, f)
        logger.info(f"[ML] Model saved to {filepath}")
        return True
    except Exception as e:
        logger.error(f"[ML] Failed to save model: {e}")
        return False


def load_model(filepath: str = DEFAULT_MODEL_PATH) -> Optional[dict]:
    """Load a saved model bundle from disk. Returns None if not found."""
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'rb') as f:
            bundle = pickle.load(f)
        logger.info(f"[ML] Model loaded from {filepath} (trained on {bundle.get('trained_on')} jobs)")
        return bundle
    except Exception as e:
        logger.error(f"[ML] Failed to load model: {e}")
        return None


def get_model_status(filepath: str = DEFAULT_MODEL_PATH) -> dict:
    """Return metadata about the saved model without loading the full bundle."""
    if not os.path.exists(filepath):
        return {'trained': False}
    bundle = load_model(filepath)
    if bundle is None:
        return {'trained': False}
    return {
        'trained':          True,
        'trained_at':       bundle.get('trained_at'),
        'trained_on':       bundle.get('trained_on'),
        'major_delay_days': bundle.get('major_delay_days'),
        'clf_metrics':      bundle.get('clf_metrics', {}),
        'reg_metrics':      bundle.get('reg_metrics', {}),
    }
