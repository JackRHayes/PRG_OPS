"""
Oryon — Crew Management Module
Handles crew CRUD, GPS tracking, assignments, and time logging.
"""

import logging
from datetime import datetime, date
from database import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_crew_tables():
    """Create crew-related tables. Safe to call on every startup."""
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS crews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                skills      TEXT    DEFAULT '',
                phone       TEXT    DEFAULT '',
                hourly_rate REAL    DEFAULT 0,
                status      TEXT    DEFAULT 'Available',
                created_at  TEXT    DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS crew_assignments (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                crew_id       INTEGER NOT NULL,
                job_id        TEXT    NOT NULL,
                assigned_date TEXT    DEFAULT CURRENT_TIMESTAMP,
                status        TEXT    DEFAULT 'Active',
                notes         TEXT    DEFAULT '',
                FOREIGN KEY (crew_id) REFERENCES crews(id),
                FOREIGN KEY (job_id)  REFERENCES jobs(job_id)
            );

            CREATE TABLE IF NOT EXISTS crew_locations (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                crew_id   INTEGER NOT NULL,
                latitude  REAL    NOT NULL,
                longitude REAL    NOT NULL,
                timestamp TEXT    DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (crew_id) REFERENCES crews(id)
            );

            CREATE TABLE IF NOT EXISTS time_logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                crew_id      INTEGER NOT NULL,
                job_id       TEXT,
                clock_in     TEXT    NOT NULL,
                clock_out    TEXT,
                hours_worked REAL    DEFAULT 0,
                log_date     TEXT    DEFAULT CURRENT_DATE,
                FOREIGN KEY (crew_id) REFERENCES crews(id)
            );
        """)
        conn.commit()
        logger.info("[Crew] Tables initialised")
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Crew CRUD
# ---------------------------------------------------------------------------

def create_crew(data: dict) -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "INSERT INTO crews (name, skills, phone, hourly_rate, status) VALUES (?, ?, ?, ?, ?)",
            (
                data.get('name', '').strip(),
                data.get('skills', '').strip(),
                data.get('phone', '').strip(),
                float(data.get('hourly_rate', 0)),
                data.get('status', 'Available'),
            )
        )
        conn.commit()
        return get_crew(cur.lastrowid)
    finally:
        close_db_connection(conn)


def get_crew(crew_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM crews WHERE id = ?", (crew_id,)).fetchone()
        return dict(row) if row else None
    finally:
        close_db_connection(conn)


def get_all_crews() -> list[dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM crews ORDER BY name").fetchall()
        crews = [dict(r) for r in rows]
        # Attach current job for each crew
        for c in crews:
            assignment = conn.execute("""
                SELECT ca.job_id FROM crew_assignments ca
                WHERE ca.crew_id = ? AND ca.status = 'Active'
                ORDER BY ca.assigned_date DESC LIMIT 1
            """, (c['id'],)).fetchone()
            c['current_job'] = assignment['job_id'] if assignment else None

            # Active clock-in?
            log = conn.execute("""
                SELECT id FROM time_logs
                WHERE crew_id = ? AND clock_out IS NULL
                ORDER BY clock_in DESC LIMIT 1
            """, (c['id'],)).fetchone()
            c['clocked_in'] = log is not None
        return crews
    finally:
        close_db_connection(conn)


def update_crew(crew_id: int, data: dict) -> dict | None:
    allowed = {'name', 'skills', 'phone', 'hourly_rate', 'status'}
    safe = {k: v for k, v in data.items() if k in allowed}
    if not safe:
        return get_crew(crew_id)
    set_clause = ', '.join(f"{k} = ?" for k in safe)
    conn = get_db_connection()
    try:
        conn.execute(
            f"UPDATE crews SET {set_clause} WHERE id = ?",
            [*safe.values(), crew_id]
        )
        conn.commit()
        return get_crew(crew_id)
    finally:
        close_db_connection(conn)


def delete_crew(crew_id: int) -> bool:
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM crews WHERE id = ?", (crew_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error("[Crew] delete_crew: %s", e)
        return False
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Assignments
# ---------------------------------------------------------------------------

def assign_crew_to_job(crew_id: int, job_id: str, notes: str = '') -> dict:
    conn = get_db_connection()
    try:
        # Mark previous active assignment for this crew as completed
        conn.execute("""
            UPDATE crew_assignments SET status = 'Completed'
            WHERE crew_id = ? AND status = 'Active'
        """, (crew_id,))
        cur = conn.execute(
            "INSERT INTO crew_assignments (crew_id, job_id, notes) VALUES (?, ?, ?)",
            (crew_id, job_id, notes)
        )
        # Update crew status
        conn.execute("UPDATE crews SET status = 'On Job' WHERE id = ?", (crew_id,))
        conn.commit()
        row = conn.execute(
            "SELECT * FROM crew_assignments WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def get_crews_for_job(job_id: str) -> list[dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT c.*, ca.assigned_date, ca.status as assignment_status, ca.notes
            FROM crew_assignments ca
            JOIN crews c ON c.id = ca.crew_id
            WHERE ca.job_id = ?
            ORDER BY ca.assigned_date DESC
        """, (job_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# GPS Location
# ---------------------------------------------------------------------------

def update_location(crew_id: int, latitude: float, longitude: float) -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "INSERT INTO crew_locations (crew_id, latitude, longitude) VALUES (?, ?, ?)",
            (crew_id, latitude, longitude)
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM crew_locations WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def get_all_locations() -> list[dict]:
    """Return the most recent location for every crew that has reported one."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT cl.crew_id, c.name, cl.latitude, cl.longitude, cl.timestamp
            FROM crew_locations cl
            JOIN crews c ON c.id = cl.crew_id
            WHERE cl.id IN (
                SELECT MAX(id) FROM crew_locations GROUP BY crew_id
            )
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Time Logging
# ---------------------------------------------------------------------------

def clock_in(crew_id: int, job_id: str | None = None) -> dict:
    conn = get_db_connection()
    try:
        # Close any open log first (safety)
        conn.execute("""
            UPDATE time_logs SET clock_out = ?, hours_worked = 0
            WHERE crew_id = ? AND clock_out IS NULL
        """, (datetime.utcnow().isoformat(), crew_id))

        now = datetime.utcnow().isoformat()
        cur = conn.execute(
            "INSERT INTO time_logs (crew_id, job_id, clock_in, log_date) VALUES (?, ?, ?, ?)",
            (crew_id, job_id, now, date.today().isoformat())
        )
        conn.execute("UPDATE crews SET status = 'On Job' WHERE id = ?", (crew_id,))
        conn.commit()
        row = conn.execute("SELECT * FROM time_logs WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def clock_out(crew_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        log = conn.execute("""
            SELECT tl.*, c.hourly_rate FROM time_logs tl
            JOIN crews c ON c.id = tl.crew_id
            WHERE tl.crew_id = ? AND tl.clock_out IS NULL
            ORDER BY tl.clock_in DESC LIMIT 1
        """, (crew_id,)).fetchone()
        if not log:
            return None

        now = datetime.utcnow()
        clock_in_dt = datetime.fromisoformat(log['clock_in'])
        hours = round((now - clock_in_dt).total_seconds() / 3600, 2)

        conn.execute("""
            UPDATE time_logs SET clock_out = ?, hours_worked = ?
            WHERE id = ?
        """, (now.isoformat(), hours, log['id']))

        # Update labor_cost on the job if assigned
        if log['job_id']:
            conn.execute("""
                UPDATE jobs
                SET labor_cost = labor_cost + ?,
                    actual_cost = actual_cost + ?,
                    updated_at = ?
                WHERE job_id = ?
            """, (
                round(hours * log['hourly_rate'], 2),
                round(hours * log['hourly_rate'], 2),
                now.isoformat(),
                log['job_id']
            ))

        conn.execute("UPDATE crews SET status = 'Available' WHERE id = ?", (crew_id,))
        conn.commit()

        row = conn.execute("SELECT * FROM time_logs WHERE id = ?", (log['id'],)).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def get_crew_hours(crew_id: int) -> dict:
    conn = get_db_connection()
    try:
        today = date.today().isoformat()
        week_start = date.fromisocalendar(
            date.today().isocalendar()[0],
            date.today().isocalendar()[1], 1
        ).isoformat()

        today_h = conn.execute("""
            SELECT COALESCE(SUM(hours_worked), 0) FROM time_logs
            WHERE crew_id = ? AND log_date = ?
        """, (crew_id, today)).fetchone()[0]

        week_h = conn.execute("""
            SELECT COALESCE(SUM(hours_worked), 0) FROM time_logs
            WHERE crew_id = ? AND log_date >= ?
        """, (crew_id, week_start)).fetchone()[0]

        total_h = conn.execute("""
            SELECT COALESCE(SUM(hours_worked), 0) FROM time_logs
            WHERE crew_id = ?
        """, (crew_id,)).fetchone()[0]

        logs = conn.execute("""
            SELECT tl.*, j.contractor as job_contractor
            FROM time_logs tl
            LEFT JOIN jobs j ON j.job_id = tl.job_id
            WHERE tl.crew_id = ?
            ORDER BY tl.clock_in DESC LIMIT 50
        """, (crew_id,)).fetchall()

        crew = get_crew(crew_id)
        labor_cost_total = round(total_h * (crew['hourly_rate'] if crew else 0), 2)

        return {
            'today_hours':    round(today_h, 2),
            'week_hours':     round(week_h, 2),
            'total_hours':    round(total_h, 2),
            'labor_cost_total': labor_cost_total,
            'logs':           [dict(r) for r in logs],
        }
    finally:
        close_db_connection(conn)


def seed_sample_crews():
    """Insert sample crew data if the crews table is empty."""
    conn = get_db_connection()
    try:
        count = conn.execute("SELECT COUNT(*) FROM crews").fetchone()[0]
        if count > 0:
            return
    finally:
        close_db_connection(conn)

    samples = [
        {'name': 'Mike Torres',    'skills': 'Civil, Gas',          'phone': '917-555-0101', 'hourly_rate': 45},
        {'name': 'Sarah Chen',     'skills': 'Water, Civil',         'phone': '917-555-0102', 'hourly_rate': 48},
        {'name': 'James Okafor',   'skills': 'Gas, Electric',        'phone': '917-555-0103', 'hourly_rate': 52},
        {'name': 'Linda Reyes',    'skills': 'Fiber, Civil',         'phone': '917-555-0104', 'hourly_rate': 44},
        {'name': 'Derek Walsh',    'skills': 'Sewer, Excavation',    'phone': '917-555-0105', 'hourly_rate': 46},
        {'name': 'Priya Nair',     'skills': 'Traffic Control',      'phone': '917-555-0106', 'hourly_rate': 40},
        {'name': 'Carlos Mendez',  'skills': 'Gas Main, Civil',      'phone': '917-555-0107', 'hourly_rate': 50},
        {'name': 'Aisha Robinson', 'skills': 'Electric, Fiber',      'phone': '917-555-0108', 'hourly_rate': 47},
    ]
    for s in samples:
        create_crew(s)
    logger.info("[Crew] Seeded %d sample crews", len(samples))
