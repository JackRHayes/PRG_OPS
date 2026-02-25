#!/usr/bin/env python3
"""
PRG Risk Intelligence — Flask Web App
Run: python3 app.py
Then open: http://127.0.0.1:5000
"""

import sys
import os
import io
import json
import tempfile
from datetime import date

from flask import Flask, request, jsonify, send_from_directory

from ingestion import load_jobs, validate_jobs
from schedule_analysis import enrich_jobs_with_schedule, aggregate_delays
from compliance_analysis import calculate_compliance_metrics, identify_repeat_issue_jobs
from contractor_scoring import calculate_contractor_scores, get_ranked_contractors
from risk_engine import score_active_jobs, get_high_risk_jobs
from rfi_analysis import (
    load_rfis, load_submittals, enrich_rfis, enrich_submittals,
    get_rfi_summary, get_submittal_summary,
    get_rfi_by_job, get_submittals_by_job, get_high_rfi_jobs
)
from permit_analysis import (
    load_permits, enrich_permits,
    get_permit_summary, get_permits_by_job,
    get_blocked_jobs, get_expiring_permits, get_permits_by_type
)
from email_alerts import send_weekly_report
from sheets_integration import get_connector, extract_spreadsheet_id, convert_sheet_to_jobs, create_default_column_mapping
from predictive_model import train_model, save_model, load_model, get_model_status, DEFAULT_MODEL_PATH
from email_scheduler import (
    send_weekly_email, generate_weekly_report, preview_report,
    load_config, save_config, schedule_weekly_reports, stop_scheduler,
)

app = Flask(__name__, static_folder='static')
app.secret_key = os.urandom(24)  # For session management

LAST_SESSION_PATH    = os.path.join(os.path.dirname(__file__), 'outputs', 'last_session.json')
PREV_SESSION_PATH    = os.path.join(os.path.dirname(__file__), 'outputs', 'prev_session.json')
RISK_HISTORY_PATH    = os.path.join(os.path.dirname(__file__), 'outputs', 'risk_history.json')
NOTES_PATH           = os.path.join(os.path.dirname(__file__), 'outputs', 'notes.json')


def save_risk_history(summary: dict):
    """Append current run's risk counts to a rolling 30-entry history file."""
    try:
        from datetime import datetime as _dt
        os.makedirs('outputs', exist_ok=True)
        history = []
        if os.path.exists(RISK_HISTORY_PATH):
            with open(RISK_HISTORY_PATH) as f:
                history = json.load(f)
        history.append({
            'timestamp': _dt.now().isoformat(),
            'high':   summary.get('high_risk_count', 0),
            'medium': summary.get('medium_risk_count', 0),
            'low':    summary.get('low_risk_count', 0),
            'total':  summary.get('total_jobs', 0),
        })
        history = history[-30:]
        with open(RISK_HISTORY_PATH, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        print(f"[HISTORY] Failed to save: {e}")


def save_session(data: dict):
    try:
        os.makedirs('outputs', exist_ok=True)
        import shutil
        if os.path.exists(LAST_SESSION_PATH):
            shutil.copy2(LAST_SESSION_PATH, PREV_SESSION_PATH)
        with open(LAST_SESSION_PATH, 'w') as f:
            json.dump(data, f, default=serialize)
        save_risk_history(data.get('summary', {}))
    except Exception as e:
        print(f"[SESSION] Failed to save: {e}")


def serialize(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def run_pipeline(file_path, rfi_path=None, sub_path=None, permit_path=None):
    rows = load_jobs(file_path)
    clean, errors = validate_jobs(rows)

    jobs = enrich_jobs_with_schedule(clean)
    delay_aggs = aggregate_delays(jobs)
    compliance = calculate_compliance_metrics(jobs)
    contractor_scores = calculate_contractor_scores(jobs)
    ranked = get_ranked_contractors(contractor_scores)
    scored_jobs = score_active_jobs(jobs, contractor_scores)

    completed = [j for j in jobs if j['status'] == 'Completed']
    active = [j for j in jobs if j['status'] in ('Open', 'In Progress')]
    avg_delay = round(sum(j.get('delay_days', 0) for j in completed) / len(completed), 1) if completed else 0

    high = [j for j in scored_jobs if j['risk_level'] == 'HIGH']
    med  = [j for j in scored_jobs if j['risk_level'] == 'MEDIUM']
    low  = [j for j in scored_jobs if j['risk_level'] == 'LOW']

    region_risk = {}
    for j in scored_jobs:
        r = j.get('region', 'Unknown')
        if r not in region_risk:
            region_risk[r] = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        region_risk[r][j['risk_level']] += 1

    rfi_data = {}
    if rfi_path:
        rfis = enrich_rfis(load_rfis(rfi_path))
        submittals = enrich_submittals(load_submittals(sub_path)) if sub_path else []
        rfi_by_job = get_rfi_by_job(rfis)
        sub_by_job = get_submittals_by_job(submittals)
        rfi_data = {
            'rfis': rfis,
            'submittals': submittals,
            'rfi_summary': get_rfi_summary(rfis),
            'submittal_summary': get_submittal_summary(submittals),
            'rfi_by_job': rfi_by_job,
            'sub_by_job': sub_by_job,
            'high_rfi_jobs': get_high_rfi_jobs(rfi_by_job),
        }

    permit_data = {}
    if permit_path:
        permits = enrich_permits(load_permits(permit_path))
        permits_by_job = get_permits_by_job(permits)
        permit_data = {
            'permits': permits,
            'permit_summary': get_permit_summary(permits),
            'permits_by_job': permits_by_job,
            'blocked_jobs': get_blocked_jobs(permits_by_job),
            'expiring_permits': get_expiring_permits(permits),
            'permits_by_type': get_permits_by_type(permits),
        }

    return {
        'summary': {
            'total_jobs': len(jobs),
            'active_jobs': len(active),
            'completed_jobs': len(completed),
            'invalid_records': len(errors),
            'avg_delay_days': avg_delay,
            'high_risk_count': len(high),
            'medium_risk_count': len(med),
            'low_risk_count': len(low),
        },
        'scored_jobs': scored_jobs,
        'all_jobs': jobs,
        'ranked_contractors': ranked,
        'compliance': compliance,
        'delay_by_contractor': delay_aggs['by_contractor'],
        'delay_by_region': delay_aggs['by_region'],
        'region_risk': region_risk,
        'validation_errors': [
            {'job_id': e.get('job_id', 'UNKNOWN'), 'errors': e.get('errors', [])}
            for e in errors
        ],
        **rfi_data,
        **permit_data,
    }


def save_temp(f):
    tmp = tempfile.NamedTemporaryFile(suffix=os.path.splitext(f.filename)[1], delete=False)
    f.save(tmp.name)
    tmp.close()
    return tmp.name


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/last-session')
def last_session():
    if os.path.exists(LAST_SESSION_PATH):
        try:
            with open(LAST_SESSION_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'No saved session'}), 404


@app.route('/api/prev-session')
def prev_session():
    if os.path.exists(PREV_SESSION_PATH):
        try:
            with open(PREV_SESSION_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'No previous session'}), 404


@app.route('/api/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    f = request.files['file']
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ('.csv', '.xlsx', '.xls'):
        return jsonify({'error': 'Only CSV or XLSX files supported'}), 400
    tmp_paths = {}
    try:
        tmp_paths['jobs'] = save_temp(f)
        for key in ('rfi_file', 'sub_file', 'permit_file'):
            ff = request.files.get(key)
            if ff and ff.filename:
                tmp_paths[key] = save_temp(ff)
        result = run_pipeline(
            tmp_paths['jobs'],
            rfi_path=tmp_paths.get('rfi_file'),
            sub_path=tmp_paths.get('sub_file'),
            permit_path=tmp_paths.get('permit_file'),
        )
        save_session(result)
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        for p in tmp_paths.values():
            try: os.unlink(p)
            except: pass


@app.route('/api/sample')
def sample():
    base = os.path.dirname(__file__)
    paths = {
        'jobs':   os.path.join(base, 'data', 'sample_jobs.csv'),
        'rfi':    os.path.join(base, 'data', 'sample_rfis.csv'),
        'sub':    os.path.join(base, 'data', 'sample_submittals.csv'),
        'permit': os.path.join(base, 'data', 'sample_permits.csv'),
    }
    if not os.path.exists(paths['jobs']):
        return jsonify({'error': 'Sample data not found. Run generate_sample_data.py first.'}), 404
    try:
        result = run_pipeline(
            paths['jobs'],
            rfi_path=paths['rfi'] if os.path.exists(paths['rfi']) else None,
            sub_path=paths['sub'] if os.path.exists(paths['sub']) else None,
            permit_path=paths['permit'] if os.path.exists(paths['permit']) else None,
        )
        save_session(result)
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Google Sheets Integration ─────────────────────────────────────────────────
from flask import session, redirect, url_for

@app.route('/api/sheets/connect')
def sheets_connect():
    """Initiate Google Sheets OAuth flow."""
    try:
        connector = get_connector()
        redirect_uri = 'http://localhost:5000/oauth2callback'
        auth_url = connector.get_auth_url(redirect_uri)
        return jsonify({'auth_url': auth_url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/oauth2callback')
def sheets_callback():
    """Handle OAuth callback from Google."""
    code = request.args.get('code')
    if not code:
        return '<h1>Authorization failed</h1><p>No authorization code received.</p>', 400
    
    connector = get_connector()
    redirect_uri = 'http://localhost:5000/oauth2callback'
    success = connector.handle_oauth_callback(code, redirect_uri)
    
    if success:
        return '<h1>✓ Connected!</h1><p>Google Sheets connected successfully. You can close this window and return to the app.</p>'
    else:
        return '<h1>Connection failed</h1><p>Unable to connect to Google Sheets.</p>', 500


@app.route('/api/sheets/status')
def sheets_status():
    """Check if Google Sheets is connected."""
    connector = get_connector()
    return jsonify({'connected': connector.is_authenticated()})


@app.route('/api/sheets/disconnect', methods=['POST'])
def sheets_disconnect():
    """Disconnect Google Sheets."""
    connector = get_connector()
    connector.disconnect()
    return jsonify({'message': 'Disconnected'})


@app.route('/api/sheets/preview', methods=['POST'])
def sheets_preview():
    """Preview spreadsheet data and suggest column mapping."""
    data = request.get_json()
    sheet_url = data.get('url')
    
    if not sheet_url:
        return jsonify({'error': 'URL required'}), 400
    
    spreadsheet_id = extract_spreadsheet_id(sheet_url)
    if not spreadsheet_id:
        return jsonify({'error': 'Invalid Google Sheets URL'}), 400
    
    connector = get_connector()
    if not connector.is_authenticated():
        return jsonify({'error': 'Not authenticated'}), 401
    
    # Get spreadsheet info
    try:
        info = connector.get_spreadsheet_info(spreadsheet_id)
    except Exception as e:
        return jsonify({'error': f'Unable to access spreadsheet: {e}'}), 500
    if not info:
        return jsonify({'error': 'Unable to access spreadsheet'}), 500
    
    # Read first sheet
    sheet_name = info['sheets'][0] if info['sheets'] else 'Sheet1'
    rows = connector.read_sheet(spreadsheet_id, f"{sheet_name}!A1:Z1000")
    
    if not rows or len(rows) < 2:
        return jsonify({'error': 'Spreadsheet is empty or has no data'}), 400
    
    # Suggest column mapping
    headers = rows[0]
    suggested_mapping = create_default_column_mapping(headers)
    
    return jsonify({
        'spreadsheet_id': spreadsheet_id,
        'title': info['title'],
        'sheet_name': sheet_name,
        'headers': headers,
        'suggested_mapping': suggested_mapping,
        'row_count': len(rows) - 1
    })


@app.route('/api/sheets/import', methods=['POST'])
def sheets_import():
    """Import data from Google Sheets using column mapping."""
    data = request.get_json()
    spreadsheet_id = data.get('spreadsheet_id')
    sheet_name = data.get('sheet_name')
    column_mapping = data.get('column_mapping')
    
    if not all([spreadsheet_id, sheet_name, column_mapping]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    connector = get_connector()
    if not connector.is_authenticated():
        return jsonify({'error': 'Not authenticated'}), 401
    
    # Read sheet data
    rows = connector.read_sheet(spreadsheet_id, f"{sheet_name}!A1:Z1000")
    if not rows:
        return jsonify({'error': 'Unable to read sheet data'}), 500
    
    # Convert to jobs using mapping
    jobs = convert_sheet_to_jobs(rows, column_mapping)
    
    # Write to temporary CSV and process through pipeline
    import tempfile
    import csv
    
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
    writer = csv.DictWriter(tmp, fieldnames=list(column_mapping.keys()))
    writer.writeheader()
    writer.writerows(jobs)
    tmp.close()
    
    try:
        result = run_pipeline(tmp.name)
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        os.unlink(tmp.name)


# ── Email Alerts ──────────────────────────────────────────────────────────────
@app.route('/api/send-test-email', methods=['POST'])
def send_test_email():
    """Send a test email with current data."""
    try:
        data = request.get_json()
        recipient = data.get('email')
        report_data = data.get('data')
        
        if not recipient:
            return jsonify({'error': 'Email address required'}), 400
        if not report_data:
            return jsonify({'error': 'No data provided'}), 400
        
        success = send_weekly_report(report_data, recipient)
        
        if success:
            return jsonify({'message': f'Test email sent to {recipient}'})
        else:
            return jsonify({'error': 'Failed to send email. Check SMTP configuration.'}), 500
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── PDF Export ────────────────────────────────────────────────────────────────
from flask import send_file
import pickle

@app.route('/api/export-pdf', methods=['POST'])
def export_pdf():
    from pdf_report import generate_pdf
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        pdf_bytes = generate_pdf(data)
        buffer = io.BytesIO(pdf_bytes)
        buffer.seek(0)
        today = date.today().strftime('%Y-%m-%d')
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'PRG_Risk_Report_{today}.pdf'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── ML Model ──────────────────────────────────────────────────────────────────
@app.route('/api/train-model', methods=['POST'])
def train_model_route():
    """Train delay prediction model on all completed jobs in the uploaded dataset."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    all_jobs = data.get('all_jobs', [])
    contractor_scores = {
        c['contractor']: c for c in data.get('ranked_contractors', [])
    }

    bundle = train_model(all_jobs, contractor_scores)
    if bundle is None:
        return jsonify({'error': 'Not enough completed jobs to train (need at least 10)'}), 400

    save_model(bundle, DEFAULT_MODEL_PATH)

    return jsonify({
        'message': f"Model trained on {bundle['trained_on']} completed jobs",
        'trained_at': bundle['trained_at'],
        'clf_metrics': bundle['clf_metrics'],
        'reg_metrics': bundle['reg_metrics'],
    })


@app.route('/api/model-status')
def model_status_route():
    """Return metadata about the currently saved model."""
    return jsonify(get_model_status(DEFAULT_MODEL_PATH))


# ── Email Scheduler ────────────────────────────────────────────────────────────
_last_dashboard_data = {}

@app.route('/api/email-settings', methods=['GET'])
def get_email_settings():
    return jsonify(load_config())


@app.route('/api/email-settings', methods=['POST'])
def update_email_settings():
    updates = request.get_json()
    if not updates:
        return jsonify({'error': 'No data provided'}), 400
    cfg = load_config()
    for key in ('recipients', 'send_day', 'send_time', 'enabled', 'smtp_user', 'smtp_pass', 'anthropic_key'):
        if key in updates:
            cfg[key] = updates[key]
    save_config(cfg)
    # Restart scheduler if enabled
    if cfg.get('enabled'):
        schedule_weekly_reports(lambda: _last_dashboard_data)
    else:
        stop_scheduler()
    return jsonify({'message': 'Settings saved', 'config': cfg})


@app.route('/api/send-weekly-report', methods=['POST'])
def send_weekly_report_route():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No dashboard data provided'}), 400
    cfg = load_config()
    recipients = data.get('recipients') or cfg.get('recipients', [])
    if not recipients:
        return jsonify({'error': 'No recipients configured'}), 400
    result = send_weekly_email(recipients, data.get('data', data))
    if result['sent']:
        return jsonify({'message': f"Sent to {len(result['sent'])} recipient(s)", **result})
    return jsonify({'error': result['errors'][0] if result['errors'] else 'Send failed', **result}), 500


@app.route('/api/preview-email', methods=['POST'])
def preview_email_route():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    html = generate_weekly_report(data)
    path = preview_report(data, os.path.join(os.path.dirname(__file__), 'email_preview.html'))
    return app.response_class(response=html, mimetype='text/html')


# ── Risk History ──────────────────────────────────────────────────────────────
@app.route('/api/risk-history')
def risk_history():
    if os.path.exists(RISK_HISTORY_PATH):
        try:
            with open(RISK_HISTORY_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify([])


# ── Notes / Annotations ────────────────────────────────────────────────────────
@app.route('/api/notes', methods=['GET'])
def get_notes():
    if os.path.exists(NOTES_PATH):
        try:
            with open(NOTES_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({})


@app.route('/api/notes', methods=['POST'])
def save_note():
    body = request.get_json()
    if not body:
        return jsonify({'error': 'No data'}), 400
    job_id = (body.get('job_id') or '').strip()
    text   = (body.get('text') or '').strip()
    if not job_id:
        return jsonify({'error': 'job_id required'}), 400
    try:
        from datetime import datetime as _dt
        os.makedirs('outputs', exist_ok=True)
        notes = {}
        if os.path.exists(NOTES_PATH):
            with open(NOTES_PATH) as f:
                notes = json.load(f)
        notes[job_id] = {'text': text, 'updated_at': _dt.now().isoformat()}
        with open(NOTES_PATH, 'w') as f:
            json.dump(notes, f, indent=2)
        return jsonify({'ok': True, 'job_id': job_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── AI Assistant ───────────────────────────────────────────────────────────────
@app.route('/api/ai-query', methods=['POST'])
def ai_query():
    try:
        import anthropic as _anthropic
    except ImportError:
        return jsonify({'error': 'Anthropic SDK not installed. Run: pip install anthropic'}), 500

    body = request.get_json()
    if not body:
        return jsonify({'error': 'No data provided'}), 400

    question = (body.get('question') or '').strip()
    if not question:
        return jsonify({'error': 'No question provided'}), 400

    dash = body.get('data', {})

    cfg = load_config()
    api_key = cfg.get('anthropic_key', '') or os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'No Anthropic API key configured. Add it in Settings → AI Assistant.'}), 400

    # Build structured context from dashboard data
    summary      = dash.get('summary', {})
    scored_jobs  = dash.get('scored_jobs', [])
    ranked       = dash.get('ranked_contractors', [])
    rfi_summary  = dash.get('rfi_summary', {})
    permit_summary = dash.get('permit_summary', {})

    high_jobs = [j for j in scored_jobs if j.get('risk_level') == 'HIGH']

    job_lines = '\n'.join(
        f"  - {j.get('job_id')}: {j.get('contractor')} | {j.get('scope_type')} | {j.get('region')} "
        f"| score={j.get('risk_score')} | {j.get('risk_reasons','')}"
        for j in high_jobs[:15]
    )
    contractor_lines = '\n'.join(
        f"  #{c.get('rank')} {c.get('contractor')}: risk={c.get('contractor_risk_factor')}, "
        f"avg_delay={c.get('avg_delay_days')}d, jobs={c.get('job_count')}"
        for c in ranked[:8]
    )

    system_prompt = f"""You are PRG Risk Intelligence, an expert AI assistant embedded in a construction operations dashboard.
You have live access to the following project data. Answer concisely and actionably.

PORTFOLIO SUMMARY:
  Total jobs: {summary.get('total_jobs', 0)}
  Active: {summary.get('active_jobs', 0)} | Completed: {summary.get('completed_jobs', 0)}
  HIGH risk: {summary.get('high_risk_count', 0)} | MEDIUM: {summary.get('medium_risk_count', 0)} | LOW: {summary.get('low_risk_count', 0)}
  Avg delay (completed jobs): {summary.get('avg_delay_days', 0)} days
  Validation errors: {summary.get('invalid_records', 0)}

HIGH RISK JOBS ({len(high_jobs)} total):
{job_lines or '  None'}

CONTRACTOR RANKINGS (by risk factor):
{contractor_lines or '  No data'}
"""

    if rfi_summary:
        system_prompt += (
            f"\nRFI STATUS: {rfi_summary.get('total',0)} total, "
            f"{rfi_summary.get('open',0)} open, {rfi_summary.get('overdue',0)} overdue, "
            f"avg response {rfi_summary.get('avg_response_days',0)}d"
        )
    if permit_summary:
        system_prompt += (
            f"\nPERMIT STATUS: {permit_summary.get('total',0)} total, "
            f"{permit_summary.get('approved',0)} approved, {permit_summary.get('blocked',0)} blocked, "
            f"{permit_summary.get('expiring_soon',0)} expiring soon"
        )

    system_prompt += (
        "\n\nRespond in plain text. Be specific — reference job IDs, contractor names, and numbers "
        "from the data above. Keep answers under 200 words unless detail is truly needed."
    )

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=512,
            system=system_prompt,
            messages=[{'role': 'user', 'content': question}],
        )
        return jsonify({'answer': msg.content[0].text})
    except _anthropic.AuthenticationError:
        return jsonify({'error': 'Invalid API key. Double-check your Anthropic key in Settings.'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    os.makedirs('static', exist_ok=True)
    print("\n" + "━" * 50)
    print("  PRG Risk Intelligence — Web App")
    print("  Open: http://127.0.0.1:5000")
    print("━" * 50 + "\n")
    app.run(debug=False, port=5000)
