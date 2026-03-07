"""
Oryon — SQLite Database Layer
Sits alongside existing CSV/JSON pipeline. Does not replace it.
"""

import os
import csv
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'oryon.db')


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    """Return an open SQLite connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def close_db_connection(conn):
    """Close a database connection."""
    if conn:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def init_database():
    """Create all tables if they don't already exist. Safe to call on every startup."""
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id           TEXT    UNIQUE NOT NULL,
                contractor       TEXT,
                scope_type       TEXT,
                region           TEXT,
                start_date       TEXT,
                planned_end_date TEXT,
                actual_end_date  TEXT,
                status           TEXT,
                markout_required INTEGER DEFAULT 0,
                markout_issues   INTEGER DEFAULT 0,
                inspections_failed INTEGER DEFAULT 0,
                crew_type        TEXT,
                budget           REAL    DEFAULT 0,
                actual_cost      REAL    DEFAULT 0,
                labor_cost       REAL    DEFAULT 0,
                material_cost    REAL    DEFAULT 0,
                created_at       TEXT    DEFAULT CURRENT_TIMESTAMP,
                updated_at       TEXT    DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS contractors (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                name           TEXT    UNIQUE NOT NULL,
                risk_factor    REAL    DEFAULT 0,
                avg_delay_days REAL    DEFAULT 0,
                delay_rate     REAL    DEFAULT 0,
                jobs_completed INTEGER DEFAULT 0,
                created_at     TEXT    DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS rfis (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                rfi_id        TEXT UNIQUE,
                job_id        TEXT,
                subject       TEXT,
                status        TEXT,
                created_at    TEXT,
                response_date TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );

            CREATE TABLE IF NOT EXISTS permits (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                permit_id    TEXT UNIQUE,
                job_id       TEXT,
                permit_type  TEXT,
                status       TEXT,
                issue_date   TEXT,
                expiry_date  TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );
        """)
        conn.commit()
        logger.info("[DB] Database initialised at %s", DB_PATH)
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Jobs CRUD
# ---------------------------------------------------------------------------

def create_job(job_data: dict) -> bool:
    """Insert a job. Silently skips duplicates (INSERT OR IGNORE)."""
    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT OR IGNORE INTO jobs
                (job_id, contractor, scope_type, region,
                 start_date, planned_end_date, actual_end_date, status,
                 markout_required, markout_issues, inspections_failed, crew_type,
                 budget, actual_cost, labor_cost, material_cost)
            VALUES
                (:job_id, :contractor, :scope_type, :region,
                 :start_date, :planned_end_date, :actual_end_date, :status,
                 :markout_required, :markout_issues, :inspections_failed, :crew_type,
                 :budget, :actual_cost, :labor_cost, :material_cost)
        """, {
            'job_id':            job_data.get('job_id', ''),
            'contractor':        job_data.get('contractor', ''),
            'scope_type':        job_data.get('scope_type', ''),
            'region':            job_data.get('region', ''),
            'start_date':        str(job_data['start_date']) if job_data.get('start_date') else None,
            'planned_end_date':  str(job_data['planned_end_date']) if job_data.get('planned_end_date') else None,
            'actual_end_date':   str(job_data['actual_end_date']) if job_data.get('actual_end_date') else None,
            'status':            job_data.get('status', ''),
            'markout_required':  1 if job_data.get('markout_required') else 0,
            'markout_issues':    int(job_data.get('markout_issues', 0)),
            'inspections_failed': int(job_data.get('inspections_failed', 0)),
            'crew_type':         job_data.get('crew_type', ''),
            'budget':            float(job_data.get('budget', 0)),
            'actual_cost':       float(job_data.get('actual_cost', 0)),
            'labor_cost':        float(job_data.get('labor_cost', 0)),
            'material_cost':     float(job_data.get('material_cost', 0)),
        })
        conn.commit()
        return True
    except Exception as e:
        logger.error("[DB] create_job failed: %s", e)
        return False
    finally:
        close_db_connection(conn)


def get_job(job_id: str) -> dict | None:
    """Return a single job as a dict, or None if not found."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None
    finally:
        close_db_connection(conn)


def get_all_jobs() -> list[dict]:
    """Return all jobs as a list of dicts."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


def update_job(job_id: str, updates: dict) -> bool:
    """Update specific fields on a job. Automatically sets updated_at."""
    if not updates:
        return False
    allowed = {
        'contractor', 'scope_type', 'region', 'start_date', 'planned_end_date',
        'actual_end_date', 'status', 'markout_required', 'markout_issues',
        'inspections_failed', 'crew_type', 'budget', 'actual_cost',
        'labor_cost', 'material_cost',
    }
    safe = {k: v for k, v in updates.items() if k in allowed}
    if not safe:
        return False
    safe['updated_at'] = datetime.utcnow().isoformat()
    safe['job_id'] = job_id
    set_clause = ', '.join(f"{k} = :{k}" for k in safe if k != 'job_id')
    conn = get_db_connection()
    try:
        conn.execute(f"UPDATE jobs SET {set_clause} WHERE job_id = :job_id", safe)
        conn.commit()
        return True
    except Exception as e:
        logger.error("[DB] update_job failed: %s", e)
        return False
    finally:
        close_db_connection(conn)


def delete_job(job_id: str) -> bool:
    """Delete a job by job_id."""
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error("[DB] delete_job failed: %s", e)
        return False
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# CSV → SQLite migration
# ---------------------------------------------------------------------------

def _csv_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), 'data', filename)


def migrate_csv_to_db() -> dict:
    """
    Read existing sample CSV files and insert into the database.
    Uses INSERT OR IGNORE so it is safe to run multiple times.
    Returns counts of rows inserted per table.
    """
    counts = {'jobs': 0, 'rfis': 0, 'permits': 0}

    # ---- Jobs ---------------------------------------------------------------
    jobs_path = _csv_path('sample_jobs.csv')
    if os.path.exists(jobs_path):
        with open(jobs_path, newline='', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                ok = create_job({
                    'job_id':            row.get('job_id', '').strip(),
                    'contractor':        row.get('contractor', '').strip(),
                    'scope_type':        row.get('scope_type', '').strip(),
                    'region':            row.get('region', '').strip(),
                    'start_date':        row.get('start_date', '').strip() or None,
                    'planned_end_date':  row.get('planned_end_date', '').strip() or None,
                    'actual_end_date':   row.get('actual_end_date', '').strip() or None,
                    'status':            row.get('status', '').strip(),
                    'markout_required':  row.get('markout_required', '').strip().lower() in ('true', '1', 'yes'),
                    'markout_issues':    int(row.get('markout_issues', 0) or 0),
                    'inspections_failed': int(row.get('inspections_failed', 0) or 0),
                    'crew_type':         row.get('crew_type', '').strip(),
                })
                if ok:
                    counts['jobs'] += 1
        logger.info("[DB] Migrated %d jobs from %s", counts['jobs'], jobs_path)
    else:
        logger.warning("[DB] Jobs CSV not found at %s — skipping", jobs_path)

    # ---- RFIs ---------------------------------------------------------------
    conn = get_db_connection()
    try:
        rfis_path = _csv_path('sample_rfis.csv')
        if os.path.exists(rfis_path):
            with open(rfis_path, newline='', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO rfis
                                (rfi_id, job_id, subject, status, created_at, response_date)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            row.get('rfi_id', '').strip(),
                            row.get('job_id', '').strip(),
                            row.get('subject', '').strip(),
                            row.get('status', '').strip(),
                            row.get('submitted_date', '').strip() or None,
                            row.get('answered_date', '').strip() or None,
                        ))
                        counts['rfis'] += 1
                    except Exception:
                        pass
            conn.commit()
            logger.info("[DB] Migrated %d RFIs", counts['rfis'])

        # ---- Permits --------------------------------------------------------
        permits_path = _csv_path('sample_permits.csv')
        if os.path.exists(permits_path):
            with open(permits_path, newline='', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO permits
                                (permit_id, job_id, permit_type, status, issue_date, expiry_date)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            row.get('permit_id', '').strip(),
                            row.get('job_id', '').strip(),
                            row.get('permit_type', '').strip(),
                            row.get('status', '').strip(),
                            row.get('approved_date', '').strip() or None,
                            row.get('expiration_date', '').strip() or None,
                        ))
                        counts['permits'] += 1
                    except Exception:
                        pass
            conn.commit()
            logger.info("[DB] Migrated %d permits", counts['permits'])

    finally:
        close_db_connection(conn)

    return counts
