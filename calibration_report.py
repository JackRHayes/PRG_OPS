"""
MODULE — Calibration & Trust Report
Evaluates prediction model performance against completed jobs and
generates a business-readable trust report.
"""
import os
import pandas as pd
from predictive_model import _build_features, load_model, DEFAULT_MODEL_PATH, MAJOR_DELAY_THRESHOLD
from sklearn.metrics import accuracy_score, precision_score, recall_score, mean_absolute_error, r2_score, confusion_matrix

def generate_calibration_report(completed_jobs: list, contractor_scores: dict) -> str:
    """
    Evaluates the current model on the provided completed jobs
    and outputs a trust report string.
    """
    model_bundle = load_model(DEFAULT_MODEL_PATH)
    if not model_bundle:
        return "Model has not been trained yet. Please train the model with sufficient historical data."

    if len(completed_jobs) == 0:
        return "No completed jobs available to run calibration."

    # Need features and true labels
    df = pd.DataFrame(completed_jobs)
    X = _build_features(df, contractor_scores)
    
    if 'delay_days' not in df.columns:
        return "Warning: Completed jobs do not contain 'delay_days' required for calibration."
        
    y_reg = df['delay_days'].fillna(0).clip(lower=0).astype(float)
    y_cls = (y_reg >= MAJOR_DELAY_THRESHOLD).astype(int)

    # Regressor metrics
    reg = model_bundle.get('regressor')
    if reg:
        preds_reg = reg.predict(X)
        mae = round(mean_absolute_error(y_reg, preds_reg), 2)
        r2 = round(r2_score(y_reg, preds_reg), 2)
    else:
        mae, r2 = None, None

    # Classifier metrics
    clf = model_bundle.get('classifier')
    if clf:
        preds_cls = clf.predict(X)
        acc = round(accuracy_score(y_cls, preds_cls), 2)
        prec = round(precision_score(y_cls, preds_cls, zero_division=0), 2)
        rec = round(recall_score(y_cls, preds_cls, zero_division=0), 2)
        cm = confusion_matrix(y_cls, preds_cls)
        # cm is [[TN, FP], [FN, TP]] if both classes present
        if cm.size == 4:
            tn, fp, fn, tp = cm.ravel()
        else:
            tn, fp, fn, tp = "N/A", "N/A", "N/A", "N/A"
    else:
        acc, prec, rec = None, None, None
        tn, fp, fn, tp = "N/A", "N/A", "N/A", "N/A"

    n_trained = model_bundle.get('trained_on', 0)
    
    # Trust Rules
    trust_level = "LOW"
    trust_reason = f"Model has only been trained on {n_trained} jobs."
    if n_trained >= 50:
        trust_level = "HIGH"
        trust_reason = "Model is highly calibrated with a rich history of completed jobs."
    elif n_trained >= 20:
        trust_level = "MEDIUM"
        trust_reason = "Model has baseline calibration but needs more history to improve statistical variance."

    report_lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  MODEL CALIBRATION & TRUST REPORT",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"  Total Historical Jobs Evaluated : {len(completed_jobs)}",
        f"  Model Training Sample Size      : {n_trained} jobs",
        f"  Current Overall Trust Level     : {trust_level}",
        f"  Reason                          : {trust_reason}",
        "",
        "  CLASSIFIER PERFORMANCE (Probability of Major Delay >= 30 Days)",
        f"  Accuracy  : {acc if acc is not None else 'N/A'}",
        f"  Precision : {prec if prec is not None else 'N/A'} (When it predicts delay, how often is it right?)",
        f"  Recall    : {rec if rec is not None else 'N/A'} (Out of all real delays, how many did it catch?)",
        ""
    ]
    if str(tp) != "N/A":
        report_lines.extend([
            "  Confusion Matrix:",
            f"    True Positives (Correctly caught delay)    : {tp}",
            f"    True Negatives (Correctly cleared job)     : {tn}",
            f"    False Positives (False alarm)              : {fp}",
            f"    False Negatives (Missed actual delay)      : {fn}",
            ""
        ])
    
    report_lines.extend([
        "  REGRESSOR PERFORMANCE (Expected Delay Days)",
        f"  MAE (Mean Absolute Error)     : +/- {mae} days" if mae is not None else "  MAE: N/A",
        f"  R² Score (Variance Explained) : {r2}" if r2 is not None else "  R² Score: N/A",
        "",
        "  GUIDANCE FOR OPERATIONS TEAMS:",
        "  - Use HIGH risk flags to prioritize job site visits.",
        "  - The Expected Exposure metric translates risk to potential bottom-line impact.",
        "  - If Trust Level is LOW, predictions act as heuristics rather than statistical certainties.",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ])
    
    return "\n".join(report_lines)

def get_calibration_metrics(completed_jobs: list, contractor_scores: dict) -> dict:
    """Returns calibration data as a structured dict for API/web consumption."""
    model_bundle = load_model(DEFAULT_MODEL_PATH)
    if not model_bundle:
        return {"error": "Model has not been trained yet. Please train the model with sufficient historical data."}

    if len(completed_jobs) == 0:
        return {"error": "No completed jobs available to run calibration."}

    df = pd.DataFrame(completed_jobs)
    X = _build_features(df, contractor_scores)

    if 'delay_days' not in df.columns:
        return {"error": "Completed jobs do not contain 'delay_days' required for calibration."}

    y_reg = df['delay_days'].fillna(0).clip(lower=0).astype(float)
    y_cls = (y_reg >= MAJOR_DELAY_THRESHOLD).astype(int)

    reg = model_bundle.get('regressor')
    if reg:
        preds_reg = reg.predict(X)
        mae = round(mean_absolute_error(y_reg, preds_reg), 2)
        r2 = round(r2_score(y_reg, preds_reg), 2)
    else:
        mae, r2 = None, None

    clf = model_bundle.get('classifier')
    if clf:
        preds_cls = clf.predict(X)
        acc = round(accuracy_score(y_cls, preds_cls), 2)
        prec = round(precision_score(y_cls, preds_cls, zero_division=0), 2)
        rec = round(recall_score(y_cls, preds_cls, zero_division=0), 2)
        cm = confusion_matrix(y_cls, preds_cls)
        if cm.size == 4:
            tn, fp, fn, tp = cm.ravel()
            tn, fp, fn, tp = int(tn), int(fp), int(fn), int(tp)
        else:
            tn, fp, fn, tp = None, None, None, None
    else:
        acc, prec, rec = None, None, None
        tn, fp, fn, tp = None, None, None, None

    n_trained = model_bundle.get('trained_on', 0)

    trust_level = "LOW"
    trust_reason = f"Model has only been trained on {n_trained} jobs."
    if n_trained >= 50:
        trust_level = "HIGH"
        trust_reason = "Model is highly calibrated with a rich history of completed jobs."
    elif n_trained >= 20:
        trust_level = "MEDIUM"
        trust_reason = "Model has baseline calibration but needs more history to improve statistical variance."

    return {
        "trained_on": n_trained,
        "n_evaluated": len(completed_jobs),
        "major_delay_threshold": MAJOR_DELAY_THRESHOLD,
        "trust_level": trust_level,
        "trust_reason": trust_reason,
        "acc": acc,
        "prec": prec,
        "rec": rec,
        "mae": mae,
        "r2": r2,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def write_calibration_report(completed_jobs: list, contractor_scores: dict, out_path: str = "outputs/calibration_report.txt"):
    report_str = generate_calibration_report(completed_jobs, contractor_scores)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write(report_str)
    return report_str
