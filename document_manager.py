"""
Oryon — Document Manager Module
File upload, storage, and retrieval per job.
"""

import os
import re
import logging
from datetime import datetime
from database import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)

DOCS_ROOT   = os.path.join(os.path.dirname(__file__), 'data', 'documents')
MAX_BYTES   = 10 * 1024 * 1024   # 10 MB
ALLOWED_EXT = {'.pdf', '.jpg', '.jpeg', '.png', '.doc', '.docx', '.xls', '.xlsx'}
CATEGORIES  = ['Permits', 'RFIs', 'Photos', 'Contracts', 'Drawings', 'Other']


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_document_tables():
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS documents (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id       TEXT    NOT NULL,
                filename     TEXT    NOT NULL,
                filepath     TEXT    NOT NULL,
                category     TEXT    DEFAULT 'Other',
                file_size    INTEGER DEFAULT 0,
                uploaded_by  TEXT    DEFAULT '',
                upload_date  TEXT    DEFAULT CURRENT_TIMESTAMP,
                notes        TEXT    DEFAULT '',
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );
        """)
        conn.commit()
        os.makedirs(DOCS_ROOT, exist_ok=True)
        logger.info("[Docs] Table and storage directory initialised")
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(name: str) -> str:
    """Strip dangerous characters from a filename."""
    name  = os.path.basename(name)
    name  = re.sub(r'[^\w\s\-.]', '_', name)
    name  = name.strip().replace(' ', '_')
    return name or 'upload'


def _job_dir(job_id: str) -> str:
    safe_jid = re.sub(r'[^\w\-]', '_', job_id)
    path = os.path.join(DOCS_ROOT, safe_jid)
    os.makedirs(path, exist_ok=True)
    return path


def _ext(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def save_document(job_id: str, file_storage, category: str = 'Other',
                  notes: str = '', uploaded_by: str = '') -> dict:
    """
    Persist an uploaded file and record it in the DB.
    file_storage: a Werkzeug FileStorage object.
    """
    if not file_storage or not file_storage.filename:
        raise ValueError("No file provided")

    filename = _safe_filename(file_storage.filename)
    ext      = _ext(filename)
    if ext not in ALLOWED_EXT:
        raise ValueError(f"File type '{ext}' not allowed. Accepted: {', '.join(ALLOWED_EXT)}")

    # Unique filename to avoid collisions
    ts       = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    unique_name = f"{ts}_{filename}"
    dest_dir    = _job_dir(job_id)
    filepath    = os.path.join(dest_dir, unique_name)

    file_storage.seek(0, 2)          # seek to end
    size = file_storage.tell()
    file_storage.seek(0)
    if size > MAX_BYTES:
        raise ValueError(f"File too large ({size//1024//1024}MB). Max 10MB.")

    file_storage.save(filepath)

    conn = get_db_connection()
    try:
        cur = conn.execute("""
            INSERT INTO documents (job_id, filename, filepath, category, file_size, uploaded_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (job_id, filename, filepath,
              category if category in CATEGORIES else 'Other',
              size, uploaded_by, notes))
        conn.commit()
        row = conn.execute("SELECT * FROM documents WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_job_documents(job_id: str, category: str | None = None) -> list[dict]:
    conn = get_db_connection()
    try:
        if category:
            rows = conn.execute(
                "SELECT * FROM documents WHERE job_id = ? AND category = ? ORDER BY upload_date DESC",
                (job_id, category)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM documents WHERE job_id = ? ORDER BY upload_date DESC",
                (job_id,)
            ).fetchall()
        docs = [dict(r) for r in rows]
        for d in docs:
            d['file_size_kb'] = round(d['file_size'] / 1024, 1)
            d['ext']          = _ext(d['filename'])
        return docs
    finally:
        close_db_connection(conn)


def get_document(doc_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        return dict(row) if row else None
    finally:
        close_db_connection(conn)


def delete_document(doc_id: int) -> bool:
    doc = get_document(doc_id)
    if not doc:
        return False
    # Delete file from disk
    try:
        if os.path.exists(doc['filepath']):
            os.remove(doc['filepath'])
    except Exception as e:
        logger.warning("[Docs] Could not remove file %s: %s", doc['filepath'], e)
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
        conn.commit()
        return True
    finally:
        close_db_connection(conn)


def update_document(doc_id: int, data: dict) -> dict | None:
    allowed = {'category', 'notes'}
    safe = {k: v for k, v in data.items() if k in allowed}
    if not safe:
        return get_document(doc_id)
    set_clause = ', '.join(f"{k} = ?" for k in safe)
    conn = get_db_connection()
    try:
        conn.execute(
            f"UPDATE documents SET {set_clause} WHERE id = ?",
            [*safe.values(), doc_id]
        )
        conn.commit()
        return get_document(doc_id)
    finally:
        close_db_connection(conn)
