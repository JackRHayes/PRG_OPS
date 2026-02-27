"""
MODULE — Automated Weekly Email Reports
Generates HTML risk reports and sends them on a schedule.
"""
import os
import json
import logging
import smtplib
from datetime import datetime, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

DEFAULT_CONFIG = {
    "recipients": [],
    "send_day": "monday",
    "send_time": "08:00",
    "enabled": False,
    "last_sent": None,
    "smtp_user": "",
    "smtp_pass": "",
    "anthropic_key": "",
}

# ── Config helpers ─────────────────────────────────────────────────────────────

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
            # Fill any missing keys with defaults
            return {**DEFAULT_CONFIG, **cfg}
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()


def save_config(cfg: dict) -> bool:
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"[EMAIL] Failed to save config: {e}")
        return False


# ── HTML report generator ──────────────────────────────────────────────────────

def generate_weekly_report(data: dict) -> str:
    """Build a full HTML email body from dashboard data."""
    summary   = data.get('summary', {})
    jobs      = data.get('scored_jobs', [])
    ranked    = data.get('ranked_contractors', [])
    today_str = date.today().strftime('%B %d, %Y')

    total_active  = summary.get('active_jobs', 0)
    high_count    = summary.get('high_risk_count', 0)
    med_count     = summary.get('medium_risk_count', 0)
    low_count     = summary.get('low_risk_count', 0)
    avg_delay     = summary.get('avg_delay_days', 0)

    # Top 5 by risk score
    top5 = sorted(jobs, key=lambda j: j.get('risk_score', 0), reverse=True)[:5]

    # Key insights
    over_30     = sum(1 for j in jobs if j.get('actual_duration_days', 0) > 30)
    high_ml     = sum(1 for j in jobs if (j.get('ml_delay_probability') or 0) >= 60)
    top_contr   = ranked[0]['contractor'] if ranked else 'N/A'

    def pill(text, color, bg):
        return (f'<span style="display:inline-block;padding:2px 10px;border-radius:4px;'
                f'font-size:12px;font-weight:600;color:{color};background:{bg};">{text}</span>')

    def risk_color(level):
        return {'HIGH': '#ef4444', 'MEDIUM': '#f59e0b', 'LOW': '#10b981'}.get(level, '#6b7280')

    def risk_bg(level):
        return {'HIGH': '#fef2f2', 'MEDIUM': '#fffbeb', 'LOW': '#f0fdf4'}.get(level, '#f9fafb')

    # Top 5 table rows
    top5_rows = ''
    if top5:
        for j in top5:
            lvl    = j.get('risk_level', 'LOW')
            ml_prob = j.get('ml_delay_probability')
            ml_cell = f'{ml_prob:.0f}%' if ml_prob is not None else '—'
            top5_rows += f"""
            <tr style="border-bottom:1px solid #f1f5f9;">
              <td style="padding:10px 12px;font-family:monospace;font-size:13px;color:#1e293b;font-weight:600">{j.get('job_id','—')}</td>
              <td style="padding:10px 12px;font-size:13px;color:#334155">{j.get('contractor','—')}</td>
              <td style="padding:10px 12px;font-size:13px;color:{risk_color(lvl)};font-weight:600">{j.get('risk_score',0)}</td>
              <td style="padding:10px 12px;font-size:13px;color:#334155">{pill(lvl, risk_color(lvl), risk_bg(lvl))}</td>
              <td style="padding:10px 12px;font-size:13px;color:#334155;text-align:center">{ml_cell}</td>
              <td style="padding:10px 12px;font-size:13px;color:#334155;text-align:center">{j.get('actual_duration_days',0)}d</td>
            </tr>"""
    else:
        top5_rows = '<tr><td colspan="6" style="padding:16px;text-align:center;color:#94a3b8;font-size:13px">No active jobs this week</td></tr>'

    # Insight bullets
    insights = []
    if over_30:
        insights.append(f'<b style="color:#ef4444">{over_30}</b> job{"s" if over_30!=1 else ""} open longer than 30 days')
    if top_contr != 'N/A':
        insights.append(f'Highest-risk contractor: <b style="color:#1e293b">{top_contr}</b>')
    if high_ml:
        insights.append(f'<b style="color:#f59e0b">{high_ml}</b> job{"s" if high_ml!=1 else ""} with 60%+ ML delay probability')
    if avg_delay > 0:
        insights.append(f'Average completed job delay: <b>{avg_delay} days</b>')
    if not insights:
        insights.append('No critical issues identified this week.')

    insight_html = ''.join(
        f'<li style="padding:5px 0;color:#334155;font-size:14px;line-height:1.6">{i}</li>'
        for i in insights
    )

    # RFI & Permit strip
    rfi_s  = data.get('rfi_summary', {})
    perm_s = data.get('permit_summary', {})
    if rfi_s or perm_s:
        def stat_cell(label, value, color):
            return (f'<td style="width:25%;padding:0 8px;text-align:center">'
                    f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:14px 8px">'
                    f'<div style="font-size:26px;font-weight:800;color:{color};line-height:1">{value}</div>'
                    f'<div style="font-size:10px;color:#94a3b8;margin-top:4px;text-transform:uppercase;letter-spacing:1px">{label}</div>'
                    f'</div></td>')
        cells = ''
        if rfi_s:
            cells += stat_cell('Open RFIs',    rfi_s.get('open', 0),    '#f59e0b' if rfi_s.get('open', 0) else '#10b981')
            cells += stat_cell('Overdue RFIs', rfi_s.get('overdue', 0), '#ef4444' if rfi_s.get('overdue', 0) else '#10b981')
        if perm_s:
            cells += stat_cell('Blocked Permits',  perm_s.get('blocked', 0),       '#ef4444' if perm_s.get('blocked', 0) else '#10b981')
            cells += stat_cell('Expiring Soon', perm_s.get('expiring_soon', 0), '#f59e0b' if perm_s.get('expiring_soon', 0) else '#10b981')
        rfi_permit_html = f'''
  <tr><td style="background:#ffffff;padding:0 32px 20px;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0">
    <h2 style="margin:0 0 16px 0;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#94a3b8">RFI & Permit Status</h2>
    <table width="100%" cellpadding="0" cellspacing="0"><tr>{cells}</tr></table>
  </td></tr>'''
    else:
        rfi_permit_html = ''

    ml_note = '' if any(j.get('ml_delay_probability') is not None for j in jobs) else (
        '<p style="margin:0;padding:10px 16px;background:#f8fafc;border-radius:6px;'
        'font-size:12px;color:#94a3b8;font-family:monospace">'
        '⚠ ML model not yet trained — showing rule-based scores only. '
        'Train the model in the dashboard Settings tab.</p>'
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:32px 16px">
<tr><td>
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;margin:0 auto">

  <!-- Header -->
  <tr><td style="background:#0f172a;border-radius:12px 12px 0 0;padding:28px 32px">
    <p style="margin:0 0 4px 0;font-size:11px;letter-spacing:3px;text-transform:uppercase;color:#f59e0b;font-weight:600">PRG GROUP · OPERATIONS INTELLIGENCE</p>
    <h1 style="margin:0;font-size:24px;font-weight:800;color:#f8fafc;letter-spacing:-0.5px">Weekly Risk Report</h1>
    <p style="margin:6px 0 0 0;font-size:13px;color:#94a3b8">{today_str}</p>
  </td></tr>

  <!-- Executive Summary -->
  <tr><td style="background:#ffffff;padding:28px 32px;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0">
    <h2 style="margin:0 0 20px 0;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#94a3b8">Executive Summary</h2>
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="width:25%;padding:0 8px 0 0">
          <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:32px;font-weight:800;color:#3b82f6;line-height:1">{total_active}</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:4px;text-transform:uppercase;letter-spacing:1px">Active Jobs</div>
          </div>
        </td>
        <td style="width:25%;padding:0 8px">
          <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:32px;font-weight:800;color:#ef4444;line-height:1">{high_count}</div>
            <div style="font-size:11px;color:#ef4444;margin-top:4px;text-transform:uppercase;letter-spacing:1px">High Risk</div>
          </div>
        </td>
        <td style="width:25%;padding:0 8px">
          <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:32px;font-weight:800;color:#f59e0b;line-height:1">{med_count}</div>
            <div style="font-size:11px;color:#f59e0b;margin-top:4px;text-transform:uppercase;letter-spacing:1px">Medium Risk</div>
          </div>
        </td>
        <td style="width:25%;padding:0 0 0 8px">
          <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:32px;font-weight:800;color:#10b981;line-height:1">{low_count}</div>
            <div style="font-size:11px;color:#10b981;margin-top:4px;text-transform:uppercase;letter-spacing:1px">Low Risk</div>
          </div>
        </td>
      </tr>
    </table>
  </td></tr>

  <!-- Top 5 Jobs -->
  <tr><td style="background:#ffffff;padding:0 32px 28px;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0">
    <h2 style="margin:0 0 16px 0;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#94a3b8">Top 5 Highest Risk Jobs</h2>
    <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;border-collapse:separate">
      <thead>
        <tr style="background:#f8fafc">
          <th style="padding:10px 12px;text-align:left;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">Job ID</th>
          <th style="padding:10px 12px;text-align:left;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">Contractor</th>
          <th style="padding:10px 12px;text-align:left;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">Score</th>
          <th style="padding:10px 12px;text-align:left;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">Level</th>
          <th style="padding:10px 12px;text-align:center;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">ML Delay %</th>
          <th style="padding:10px 12px;text-align:center;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;font-weight:600">Days Open</th>
        </tr>
      </thead>
      <tbody>{top5_rows}</tbody>
    </table>
    {ml_note}
  </td></tr>

  <!-- RFI & Permit Strip -->
  {rfi_permit_html}

  <!-- Key Insights -->
  <tr><td style="background:#ffffff;padding:0 32px 28px;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0">
    <h2 style="margin:0 0 12px 0;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#94a3b8">Key Insights</h2>
    <ul style="margin:0;padding:0 0 0 18px">{insight_html}</ul>
  </td></tr>

  <!-- Footer -->
  <tr><td style="background:#0f172a;border-radius:0 0 12px 12px;padding:20px 32px">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td>
          <p style="margin:0;font-size:13px;color:#94a3b8">Generated by PRG Risk Intelligence · Reply to this email to unsubscribe.</p>
          <p style="margin:6px 0 0 0;font-size:11px;color:#475569">PRG Risk Intelligence · Automated Weekly Report</p>
        </td>
        <td style="text-align:right">
          <p style="margin:0;font-size:11px;color:#475569"><a href="#" style="color:#475569">Unsubscribe</a></p>
        </td>
      </tr>
    </table>
  </td></tr>

</table>
</td></tr>
</table>
</body></html>"""

    return html


# ── Email sender ───────────────────────────────────────────────────────────────

def send_weekly_email(recipients: List[str], data: dict) -> dict:
    """
    Send the weekly HTML report to a list of recipients.
    Returns {'sent': [...], 'failed': [...], 'errors': [...]}.
    """
    smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.environ.get('SMTP_PORT', 587))
    cfg = load_config()
    smtp_user = cfg.get('smtp_user') or os.environ.get('SMTP_USER', '')
    smtp_pass = cfg.get('smtp_pass') or os.environ.get('SMTP_PASS', '')

    if not smtp_user or not smtp_pass:
        return {'sent': [], 'failed': recipients, 'errors': ['Gmail address and app password not configured — go to Settings → Email Reports']}

    html_body = generate_weekly_report(data)
    subject   = f"Weekly Risk Report — {date.today().strftime('%B %d, %Y')}"

    sent, failed, errors = [], [], []

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            for recipient in recipients:
                try:
                    msg = MIMEMultipart('alternative')
                    msg['From']    = smtp_user
                    msg['To']      = recipient
                    msg['Subject'] = subject
                    msg.attach(MIMEText(html_body, 'html'))
                    server.sendmail(smtp_user, recipient, msg.as_string())
                    sent.append(recipient)
                    logger.info(f"[EMAIL] Sent to {recipient}")
                except Exception as e:
                    failed.append(recipient)
                    errors.append(f"{recipient}: {e}")
                    logger.error(f"[EMAIL] Failed for {recipient}: {e}")
    except Exception as e:
        failed = recipients
        errors = [str(e)]
        logger.error(f"[EMAIL] SMTP connection failed: {e}")

    # Only update last_sent if at least one email was sent successfully
    if sent:
        cfg = load_config()
        cfg['last_sent'] = datetime.now().isoformat()
        save_config(cfg)

    return {'sent': sent, 'failed': failed, 'errors': errors}


# ── Scheduler ──────────────────────────────────────────────────────────────────

_scheduler = None

def schedule_weekly_reports(get_data_fn):
    """
    Start APScheduler to send weekly reports.
    get_data_fn: a callable that returns current dashboard data dict.
    """
    global _scheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("[EMAIL] apscheduler not installed — pip install apscheduler")
        return False

    cfg = load_config()
    if not cfg.get('enabled'):
        logger.info("[EMAIL] Scheduler disabled in config.")
        return False

    day_map = {
        'monday': 'mon', 'tuesday': 'tue', 'wednesday': 'wed',
        'thursday': 'thu', 'friday': 'fri', 'saturday': 'sat', 'sunday': 'sun',
    }
    send_day  = day_map.get(cfg.get('send_day', 'monday').lower(), 'mon')
    send_time = cfg.get('send_time', '08:00')
    try:
        hour, minute = send_time.split(':')
    except (ValueError, AttributeError):
        hour, minute = 8, 0

    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)

    _scheduler = BackgroundScheduler()

    def job():
        current_cfg = load_config()
        if not current_cfg.get('enabled') or not current_cfg.get('recipients'):
            logger.info("[EMAIL] Skipping scheduled send — disabled or no recipients.")
            return
        data = get_data_fn()
        if data:
            result = send_weekly_email(current_cfg['recipients'], data)
            logger.info(f"[EMAIL] Scheduled send complete: {result}")

    _scheduler.add_job(
        job,
        CronTrigger(day_of_week=send_day, hour=int(hour), minute=int(minute)),
        id='weekly_report',
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(f"[EMAIL] Scheduler started — every {send_day} at {send_time}")
    return True


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def preview_report(data: dict, output_path: str = 'email_preview.html') -> str:
    """Write HTML preview to disk and return the path."""
    html = generate_weekly_report(data)
    with open(output_path, 'w') as f:
        f.write(html)
    return output_path
