"""
Oryon — Scheduling & Dispatch Module
Job calendar management, crew auto-assignment, and dispatch logging.
"""

import json
import logging
import os
import urllib.request
from datetime import date, datetime
from database import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_schedule_tables():
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS job_schedule (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id          TEXT    UNIQUE NOT NULL,
                scheduled_start TEXT,
                scheduled_end   TEXT,
                weather_hold    INTEGER DEFAULT 0,
                priority        TEXT    DEFAULT 'Normal',
                notes           TEXT    DEFAULT '',
                created_at      TEXT    DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT    DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );

            CREATE TABLE IF NOT EXISTS dispatch_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT    NOT NULL,
                crew_id       INTEGER NOT NULL,
                dispatched_at TEXT    DEFAULT CURRENT_TIMESTAMP,
                eta           TEXT    DEFAULT '',
                status        TEXT    DEFAULT 'Dispatched',
                notes         TEXT    DEFAULT '',
                FOREIGN KEY (job_id)  REFERENCES jobs(job_id),
                FOREIGN KEY (crew_id) REFERENCES crews(id)
            );
        """)
        conn.commit()
        logger.info("[Schedule] Tables initialised")
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------

def schedule_job(job_id: str, data: dict) -> dict:
    now = datetime.utcnow().isoformat()
    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO job_schedule
                (job_id, scheduled_start, scheduled_end, weather_hold, priority, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                scheduled_start = excluded.scheduled_start,
                scheduled_end   = excluded.scheduled_end,
                weather_hold    = excluded.weather_hold,
                priority        = excluded.priority,
                notes           = excluded.notes,
                updated_at      = excluded.updated_at
        """, (
            job_id,
            data.get('scheduled_start'),
            data.get('scheduled_end'),
            1 if data.get('weather_hold') else 0,
            data.get('priority', 'Normal'),
            data.get('notes', '').strip(),
            now,
        ))
        conn.commit()
        return get_job_schedule(job_id)
    finally:
        close_db_connection(conn)


def get_job_schedule(job_id: str) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM job_schedule WHERE job_id = ?", (job_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        close_db_connection(conn)


def get_schedule(start: str | None = None, end: str | None = None) -> list[dict]:
    """Return schedule entries joined with job info, optionally filtered by date range."""
    conn = get_db_connection()
    try:
        query = """
            SELECT js.*, j.contractor, j.scope_type, j.region, j.status AS job_status
            FROM job_schedule js
            JOIN jobs j ON j.job_id = js.job_id
        """
        params = []
        conditions = []
        if start:
            conditions.append("(js.scheduled_end IS NULL OR js.scheduled_end >= ?)")
            params.append(start)
        if end:
            conditions.append("(js.scheduled_start IS NULL OR js.scheduled_start <= ?)")
            params.append(end)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY js.scheduled_start"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


def delete_job_schedule(job_id: str) -> bool:
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM job_schedule WHERE job_id = ?", (job_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error("[Schedule] delete_job_schedule: %s", e)
        return False
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Auto-Assign Algorithm
# ---------------------------------------------------------------------------

# Maps keywords found in scope_type → crew skill terms to look for.
# Allows "Service Install" to match crews with "Gas", "Water", "Electric", etc.
_SCOPE_SKILL_MAP: dict[str, list[str]] = {
    'repair':    ['civil', 'excavation', 'sewer', 'gas', 'water', 'gas main'],
    'main':      ['gas main', 'water', 'civil', 'gas'],
    'emergency': ['civil', 'gas', 'water', 'gas main', 'excavation'],
    'service':   ['civil', 'gas', 'water', 'electric', 'fiber'],
    'install':   ['civil', 'gas', 'water', 'electric', 'fiber'],
    'upgrade':   ['civil', 'electric', 'fiber'],
    'valve':     ['gas', 'water', 'gas main', 'civil'],
    'planned':   ['civil', 'excavation'],
}


def auto_assign_crew(job_id: str) -> list[dict]:
    """
    Return up to 3 available crews ranked by skill match against the job scope_type.

    Matching strategy:
      1. Parse crew skills by comma so multi-word skills (e.g. "Gas Main") are preserved.
      2. Expand scope keywords via _SCOPE_SKILL_MAP to bridge terminology gaps
         (e.g. "Service Install" → civil, gas, water, electric, fiber).
      3. Score = (unique skill matches * 10) - (week_hours / 10)
    """
    conn = get_db_connection()
    try:
        job = conn.execute(
            "SELECT scope_type, region FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if not job:
            return []

        # Build expanded target skill set from scope keywords
        scope_lower = (job['scope_type'] or '').lower()
        scope_words = set(scope_lower.replace(',', ' ').split())
        target_skills: set[str] = set()
        for word in scope_words:
            target_skills.update(_SCOPE_SKILL_MAP.get(word, [word]))

        crews = conn.execute("""
            SELECT c.*, COALESCE(SUM(tl.hours_worked), 0) AS week_hours
            FROM crews c
            LEFT JOIN time_logs tl
                ON tl.crew_id = c.id AND tl.log_date >= date('now', '-7 days')
            WHERE c.status = 'Available'
            GROUP BY c.id
        """).fetchall()

        scored = []
        for c in crews:
            # Parse skills by comma to preserve multi-word entries like "Gas Main"
            crew_skills = {s.strip().lower() for s in (c['skills'] or '').split(',') if s.strip()}
            skill_match = len(target_skills & crew_skills)
            score = (skill_match * 10) - (c['week_hours'] / 10)
            scored.append({
                **dict(c),
                'match_score': round(score, 1),
                'skill_match': skill_match,
            })

        scored.sort(key=lambda x: x['match_score'], reverse=True)
        return scored[:3]
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def dispatch_crew(job_id: str, crew_id: int,
                  eta: str = '', notes: str = '') -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute("""
            INSERT INTO dispatch_log (job_id, crew_id, eta, notes)
            VALUES (?, ?, ?, ?)
        """, (job_id, crew_id, eta, notes))
        conn.execute("UPDATE crews SET status = 'On Job' WHERE id = ?", (crew_id,))
        conn.commit()
        row = conn.execute(
            "SELECT * FROM dispatch_log WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def get_dispatch_log(job_id: str) -> list[dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT dl.*, c.name AS crew_name, c.skills
            FROM dispatch_log dl
            JOIN crews c ON c.id = dl.crew_id
            WHERE dl.job_id = ?
            ORDER BY dl.dispatched_at DESC
        """, (job_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_schedule_stats() -> dict:
    conn = get_db_connection()
    try:
        today = date.today().isoformat()
        iso = date.today().isocalendar()
        week_end = date.fromisocalendar(iso[0], iso[1], 7).isoformat()

        this_week = conn.execute("""
            SELECT COUNT(*) FROM job_schedule
            WHERE scheduled_start <= ? AND scheduled_end >= ?
        """, (week_end, today)).fetchone()[0]

        dispatched_today = conn.execute("""
            SELECT COUNT(DISTINCT crew_id) FROM dispatch_log
            WHERE DATE(dispatched_at) = ?
        """, (today,)).fetchone()[0]

        weather_holds = conn.execute(
            "SELECT COUNT(*) FROM job_schedule WHERE weather_hold = 1"
        ).fetchone()[0]

        total_scheduled = conn.execute(
            "SELECT COUNT(*) FROM job_schedule"
        ).fetchone()[0]

        return {
            'scheduled_this_week': this_week,
            'dispatched_today':    dispatched_today,
            'weather_holds':       weather_holds,
            'total_scheduled':     total_scheduled,
        }
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Weather (OpenWeather API)
# ---------------------------------------------------------------------------

def get_weather(lat: float, lng: float, api_key: str) -> dict:
    """
    Fetch current weather from OpenWeather.
    Returns temp_f, conditions, wind_mph, humidity, icon, rain_1h, is_hold.
    is_hold is True when rain > 0.5 in/hr or wind > 25 mph.
    """
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lng}&appid={api_key}&units=imperial"
    )
    req = urllib.request.Request(url, headers={'User-Agent': 'Oryon/1.0'})
    with urllib.request.urlopen(req, timeout=6) as resp:
        data = json.loads(resp.read())

    wind_mph = data['wind']['speed']
    rain_1h  = data.get('rain', {}).get('1h', 0)

    return {
        'temp_f':      round(data['main']['temp'], 1),
        'feels_like_f': round(data['main']['feels_like'], 1),
        'conditions':  data['weather'][0]['description'].title(),
        'icon':        data['weather'][0]['icon'],
        'humidity':    data['main']['humidity'],
        'wind_mph':    round(wind_mph, 1),
        'rain_1h':     round(rain_1h, 2),
        'is_hold':     rain_1h > 0.5 or wind_mph > 25,
        'city':        data.get('name', ''),
    }
