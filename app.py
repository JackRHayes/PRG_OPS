#!/usr/bin/env python3
"""
PRG Risk Intelligence — Flask Web App
Run: python3 app.py
Then open: http://127.0.0.1:5000
"""

import sys
import os
import io
import re
import json
import logging
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
from database import init_database, get_all_jobs, create_job
from crew_management import (
    init_crew_tables, seed_sample_crews,
    create_crew, get_crew, get_all_crews, update_crew, delete_crew,
    assign_crew_to_job, get_crews_for_job,
    update_location, get_all_locations,
    clock_in, clock_out, get_crew_hours,
)
from financial_tracking import (
    init_financial_tables,
    set_budget, get_budget,
    add_expense, get_expenses, delete_expense,
    get_job_financials, get_financials_overview,
)
from document_manager import (
    init_document_tables,
    save_document, get_job_documents, get_document,
    delete_document, update_document, CATEGORIES,
)

app = Flask(__name__, static_folder='static')
# Use a stable secret key so sessions survive server restarts
app.secret_key = os.environ.get('PRG_SECRET_KEY', 'prg-ops-dev-key-change-in-production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# Initialise SQLite database and all module tables (no-op if already exists)
init_database()
init_crew_tables()
seed_sample_crews()
init_financial_tables()
init_document_tables()

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
        logger.error(f"[HISTORY] Failed to save: {e}")


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
        logger.error(f"[SESSION] Failed to save: {e}")


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
            logger.error(f"[last-session] {e}")
            return jsonify({'error': 'Internal server error'}), 500
    return jsonify({'error': 'No saved session'}), 404


@app.route('/api/prev-session')
def prev_session():
    if os.path.exists(PREV_SESSION_PATH):
        try:
            with open(PREV_SESSION_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            logger.error(f"[prev-session] {e}")
            return jsonify({'error': 'Internal server error'}), 500
    return jsonify({'error': 'No previous session'}), 404


@app.route('/api/database/status', methods=['GET'])
def database_status():
    """Check whether the SQLite database is initialised and how many jobs it holds."""
    try:
        jobs = get_all_jobs()
        return jsonify({
            'database_initialized': True,
            'job_count': len(jobs),
            'status': 'ready',
        })
    except Exception as e:
        logger.error(f"[database/status] {e}")
        return jsonify({
            'database_initialized': False,
            'error': str(e),
            'status': 'not_initialized',
        }), 500


# ---------------------------------------------------------------------------
# CREW ROUTES
# ---------------------------------------------------------------------------

@app.route('/api/crews', methods=['GET'])
def list_crews():
    try:
        return jsonify(get_all_crews())
    except Exception as e:
        logger.error(f"[crews GET] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews', methods=['POST'])
def add_crew():
    data = request.get_json(silent=True) or {}
    if not data.get('name', '').strip():
        return jsonify({'error': 'name is required'}), 400
    try:
        crew = create_crew(data)
        return jsonify(crew), 201
    except Exception as e:
        logger.error(f"[crews POST] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/<int:crew_id>', methods=['GET'])
def get_crew_route(crew_id):
    crew = get_crew(crew_id)
    if not crew:
        return jsonify({'error': 'Crew not found'}), 404
    return jsonify(crew)


@app.route('/api/crews/<int:crew_id>', methods=['PUT'])
def update_crew_route(crew_id):
    data = request.get_json(silent=True) or {}
    try:
        crew = update_crew(crew_id, data)
        return jsonify(crew)
    except Exception as e:
        logger.error(f"[crews PUT] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/<int:crew_id>', methods=['DELETE'])
def delete_crew_route(crew_id):
    ok = delete_crew(crew_id)
    return jsonify({'ok': ok})


@app.route('/api/crews/assign', methods=['POST'])
def assign_crew():
    data = request.get_json(silent=True) or {}
    crew_id = data.get('crew_id')
    job_id  = data.get('job_id', '').strip()
    if not crew_id or not job_id:
        return jsonify({'error': 'crew_id and job_id are required'}), 400
    try:
        assignment = assign_crew_to_job(int(crew_id), job_id, data.get('notes', ''))
        return jsonify(assignment), 201
    except Exception as e:
        logger.error(f"[crews/assign] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>/crews', methods=['GET'])
def job_crews(job_id):
    try:
        return jsonify(get_crews_for_job(job_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/location', methods=['POST'])
def post_location():
    data = request.get_json(silent=True) or {}
    crew_id = data.get('crew_id')
    lat     = data.get('latitude')
    lng     = data.get('longitude')
    if not all([crew_id, lat is not None, lng is not None]):
        return jsonify({'error': 'crew_id, latitude, longitude required'}), 400
    try:
        loc = update_location(int(crew_id), float(lat), float(lng))
        return jsonify(loc), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/locations', methods=['GET'])
def all_locations():
    try:
        return jsonify(get_all_locations())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/clock-in', methods=['POST'])
def crew_clock_in():
    data    = request.get_json(silent=True) or {}
    crew_id = data.get('crew_id')
    if not crew_id:
        return jsonify({'error': 'crew_id required'}), 400
    try:
        log = clock_in(int(crew_id), data.get('job_id'))
        return jsonify(log), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/clock-out', methods=['POST'])
def crew_clock_out():
    data    = request.get_json(silent=True) or {}
    crew_id = data.get('crew_id')
    if not crew_id:
        return jsonify({'error': 'crew_id required'}), 400
    try:
        log = clock_out(int(crew_id))
        if not log:
            return jsonify({'error': 'No active clock-in found'}), 404
        return jsonify(log)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crews/<int:crew_id>/hours', methods=['GET'])
def crew_hours(crew_id):
    try:
        return jsonify(get_crew_hours(crew_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# FINANCIAL ROUTES
# ---------------------------------------------------------------------------

@app.route('/api/jobs/<job_id>/budget', methods=['POST'])
def job_budget(job_id):
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(set_budget(job_id, data))
    except Exception as e:
        logger.error(f"[budget POST] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>/financials', methods=['GET'])
def job_financials(job_id):
    try:
        return jsonify(get_job_financials(job_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>/expenses', methods=['GET'])
def list_expenses(job_id):
    try:
        return jsonify(get_expenses(job_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>/expenses', methods=['POST'])
def create_expense(job_id):
    data = request.get_json(silent=True) or {}
    if not data.get('amount'):
        return jsonify({'error': 'amount is required'}), 400
    try:
        return jsonify(add_expense(job_id, data)), 201
    except Exception as e:
        logger.error(f"[expenses POST] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/expenses/<int:expense_id>', methods=['DELETE'])
def remove_expense(expense_id):
    ok = delete_expense(expense_id)
    return jsonify({'ok': ok})


@app.route('/api/financials/overview', methods=['GET'])
def financials_overview():
    try:
        return jsonify(get_financials_overview())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# DOCUMENT ROUTES
# ---------------------------------------------------------------------------

@app.route('/api/documents/upload', methods=['POST'])
def upload_document():
    job_id = request.form.get('job_id', '').strip()
    if not job_id:
        return jsonify({'error': 'job_id is required'}), 400
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    try:
        doc = save_document(
            job_id,
            request.files['file'],
            category    = request.form.get('category', 'Other'),
            notes       = request.form.get('notes', ''),
            uploaded_by = request.form.get('uploaded_by', ''),
        )
        return jsonify(doc), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"[doc upload] {e}")
        return jsonify({'error': 'Upload failed'}), 500


@app.route('/api/jobs/<job_id>/documents', methods=['GET'])
def list_documents(job_id):
    cat = request.args.get('category')
    try:
        return jsonify(get_job_documents(job_id, cat))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/documents/<int:doc_id>', methods=['GET'])
def download_document(doc_id):
    doc = get_document(doc_id)
    if not doc:
        return jsonify({'error': 'Not found'}), 404
    try:
        return send_file(doc['filepath'], as_attachment=True,
                         download_name=doc['filename'])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/documents/<int:doc_id>', methods=['PUT'])
def patch_document(doc_id):
    data = request.get_json(silent=True) or {}
    doc  = update_document(doc_id, data)
    return jsonify(doc) if doc else (jsonify({'error': 'Not found'}), 404)


@app.route('/api/documents/<int:doc_id>', methods=['DELETE'])
def remove_document(doc_id):
    ok = delete_document(doc_id)
    return jsonify({'ok': ok})


@app.route('/api/documents/categories', methods=['GET'])
def document_categories():
    return jsonify(CATEGORIES)


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
        global _last_dashboard_data
        _last_dashboard_data = result
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"[upload] {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        for p in tmp_paths.values():
            try: os.unlink(p)
            except OSError: pass


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
        global _last_dashboard_data
        _last_dashboard_data = result
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"[sample] {e}")
        return jsonify({'error': 'Internal server error'}), 500


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
        logger.error(f"[sheets/connect] {e}")
        return jsonify({'error': 'Internal server error'}), 500


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
    data = request.get_json(silent=True) or {}
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
        logger.error(f"[sheets/preview] {e}")
        return jsonify({'error': 'Unable to access spreadsheet'}), 500
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
    data = request.get_json(silent=True) or {}
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
        save_session(result)
        global _last_dashboard_data
        _last_dashboard_data = result
        return app.response_class(
            response=json.dumps(result, default=serialize),
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"[sheets/import] {e}")
        return jsonify({'error': 'Internal server error'}), 500
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
        if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', recipient):
            return jsonify({'error': 'Invalid email address format'}), 400
        if not report_data:
            return jsonify({'error': 'No data provided'}), 400

        success = send_weekly_report(report_data, recipient)

        if success:
            return jsonify({'message': f'Test email sent to {recipient}'})
        else:
            return jsonify({'error': 'Failed to send email. Check SMTP configuration.'}), 500

    except Exception as e:
        logger.error(f"[send-test-email] {e}")
        return jsonify({'error': 'Internal server error'}), 500


# ── PDF Export ────────────────────────────────────────────────────────────────
from flask import send_file

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
        logger.error(f"[export-pdf] {e}")
        return jsonify({'error': 'Internal server error'}), 500


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
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'No dashboard data provided'}), 400
    global _last_dashboard_data
    _last_dashboard_data = data.get('data', data)
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
            logger.error(f"[risk-history] {e}")
            return jsonify({'error': 'Internal server error'}), 500
    return jsonify([])


# ── Notes / Annotations ────────────────────────────────────────────────────────
@app.route('/api/notes', methods=['GET'])
def get_notes():
    if os.path.exists(NOTES_PATH):
        try:
            with open(NOTES_PATH) as f:
                return app.response_class(response=f.read(), mimetype='application/json')
        except Exception as e:
            logger.error(f"[notes GET] {e}")
            return jsonify({'error': 'Internal server error'}), 500
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
        logger.error(f"[notes POST] {e}")
        return jsonify({'error': 'Internal server error'}), 500


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
            model='claude-3-5-haiku-20241022',
            max_tokens=512,
            system=system_prompt,
            messages=[{'role': 'user', 'content': question}],
        )
        return jsonify({'answer': msg.content[0].text})
    except _anthropic.AuthenticationError:
        return jsonify({'error': 'Invalid API key. Double-check your Anthropic key in Settings.'}), 401
    except Exception as e:
        logger.error(f"[ai-query] {e}")
        return jsonify({'error': 'Internal server error'}), 500


# ── What-If / Calibration / Cost Config ───────────────────────────────────────
COST_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'cost_config.json')
DEFAULT_COST_CONFIG = {
    "cost_per_delay_day": 4000,
    "penalty_threshold_days": 30,
    "penalty_flat_fee": 50000,
}


@app.route('/api/simulate', methods=['POST'])
def simulate():
    from what_if import run_scenario
    body = request.get_json()
    if not body:
        return jsonify({'error': 'No data provided'}), 400
    job_id = (body.get('job_id') or '').strip()
    modifications = body.get('modifications', {})
    if not job_id:
        return jsonify({'error': 'job_id required'}), 400
    if not os.path.exists(LAST_SESSION_PATH):
        return jsonify({'error': 'No session data. Load data first.'}), 404
    try:
        with open(LAST_SESSION_PATH) as f:
            session_data = json.load(f)
    except Exception as e:
        logger.error(f"[simulate] Failed to read session: {e}")
        return jsonify({'error': 'Internal server error'}), 500

    all_jobs = session_data.get('all_jobs', [])
    job = next((j for j in all_jobs if j.get('job_id') == job_id), None)
    if job is None:
        scored = session_data.get('scored_jobs', [])
        job = next((j for j in scored if j.get('job_id') == job_id), None)
    if job is None:
        return jsonify({'error': f'Job {job_id} not found in session'}), 404

    contractor_scores = {
        c['contractor']: c for c in session_data.get('ranked_contractors', [])
    }
    try:
        result = run_scenario(job, contractor_scores, modifications)
        return jsonify(result)
    except Exception as e:
        logger.error(f"[simulate] {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/calibration')
def calibration():
    from calibration_report import get_calibration_metrics
    if not os.path.exists(LAST_SESSION_PATH):
        return jsonify({'error': 'No session data. Load data first.'}), 404
    try:
        with open(LAST_SESSION_PATH) as f:
            session_data = json.load(f)
    except Exception as e:
        logger.error(f"[calibration] Failed to read session: {e}")
        return jsonify({'error': 'Internal server error'}), 500

    all_jobs = session_data.get('all_jobs', [])
    completed = [j for j in all_jobs if j.get('status') == 'Completed']
    contractor_scores = {
        c['contractor']: c for c in session_data.get('ranked_contractors', [])
    }
    try:
        metrics = get_calibration_metrics(completed, contractor_scores)
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"[calibration] {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cost-config', methods=['GET'])
def get_cost_config():
    if os.path.exists(COST_CONFIG_PATH):
        try:
            with open(COST_CONFIG_PATH) as f:
                return jsonify(json.load(f))
        except Exception as e:
            logger.error(f"[cost-config GET] {e}")
            return jsonify({'error': 'Internal server error'}), 500
    return jsonify(DEFAULT_COST_CONFIG)


@app.route('/api/cost-config', methods=['POST'])
def save_cost_config():
    body = request.get_json()
    if not body:
        return jsonify({'error': 'No data provided'}), 400
    required_keys = {'cost_per_delay_day', 'penalty_threshold_days', 'penalty_flat_fee'}
    if not required_keys.issubset(body.keys()):
        return jsonify({'error': f'Missing keys. Required: {sorted(required_keys)}'}), 400
    try:
        config = {k: body[k] for k in required_keys}
        with open(COST_CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=2)
        return jsonify({'ok': True, 'config': config})
    except Exception as e:
        logger.error(f"[cost-config POST] {e}")
        return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    os.makedirs('static', exist_ok=True)
    port = int(os.environ.get('PORT', 5000))
    print("\n" + "━" * 50)
    print("  PRG Risk Intelligence — Web App")
    print(f"  Open: http://127.0.0.1:{port}")
    print("━" * 50 + "\n")
    app.run(debug=False, host='0.0.0.0', port=port)
