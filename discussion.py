"""
Oryon — Discussion Board Module
Team posts, inline comments, emoji reactions, and real-time SSE notifications.
No password auth required — author identity is set as a display name per client.
"""

import json
import logging
import queue
import threading
from datetime import datetime
from database import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SSE pub/sub (in-memory, single-process)
# ---------------------------------------------------------------------------

_sse_lock        = threading.Lock()
_sse_subscribers: list[queue.Queue] = []


def subscribe() -> queue.Queue:
    q = queue.Queue(maxsize=100)
    with _sse_lock:
        _sse_subscribers.append(q)
    return q


def unsubscribe(q: queue.Queue):
    with _sse_lock:
        if q in _sse_subscribers:
            _sse_subscribers.remove(q)


def broadcast(data: dict):
    msg = json.dumps(data)
    with _sse_lock:
        dead = []
        for q in _sse_subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_subscribers.remove(q)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_discussion_tables():
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                author_name TEXT    NOT NULL DEFAULT 'Anonymous',
                job_id      TEXT,
                title       TEXT    DEFAULT '',
                body        TEXT    NOT NULL,
                pinned      INTEGER DEFAULT 0,
                created_at  TEXT    DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS comments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id     INTEGER NOT NULL,
                author_name TEXT    NOT NULL DEFAULT 'Anonymous',
                body        TEXT    NOT NULL,
                created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id     INTEGER NOT NULL,
                author_name TEXT    NOT NULL,
                emoji       TEXT    NOT NULL,
                created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (post_id, author_name, emoji),
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
            );
        """)
        conn.commit()
        logger.info("[Discussion] Tables initialised")
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Posts
# ---------------------------------------------------------------------------

def create_post(author_name: str, body: str,
                job_id: str | None = None, title: str = '') -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "INSERT INTO posts (author_name, body, job_id, title) VALUES (?, ?, ?, ?)",
            (author_name.strip(), body.strip(), job_id or None, title.strip())
        )
        conn.commit()
        post = get_post(cur.lastrowid)
        broadcast({'type': 'new_post', 'post': post})
        return post
    finally:
        close_db_connection(conn)


def get_posts(job_id: str | None = None,
              limit: int = 60, offset: int = 0) -> list[dict]:
    conn = get_db_connection()
    try:
        if job_id:
            rows = conn.execute("""
                SELECT p.*,
                    (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) AS comment_count
                FROM posts p
                WHERE p.job_id = ?
                ORDER BY p.pinned DESC, p.created_at DESC
                LIMIT ? OFFSET ?
            """, (job_id, limit, offset)).fetchall()
        else:
            rows = conn.execute("""
                SELECT p.*,
                    (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) AS comment_count
                FROM posts p
                ORDER BY p.pinned DESC, p.created_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset)).fetchall()
        posts = [dict(r) for r in rows]
        # Attach reaction summaries
        for p in posts:
            p['reactions'] = _get_reactions_summary(p['id'], conn)
        return posts
    finally:
        close_db_connection(conn)


def get_post(post_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT p.*,
                (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) AS comment_count
            FROM posts p WHERE p.id = ?
        """, (post_id,)).fetchone()
        if not row:
            return None
        p = dict(row)
        p['reactions'] = _get_reactions_summary(p['id'], conn)
        return p
    finally:
        close_db_connection(conn)


def delete_post(post_id: int) -> bool:
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
        conn.commit()
        broadcast({'type': 'delete_post', 'post_id': post_id})
        return True
    except Exception as e:
        logger.error("[Discussion] delete_post: %s", e)
        return False
    finally:
        close_db_connection(conn)


def pin_post(post_id: int, pinned: bool = True) -> dict | None:
    conn = get_db_connection()
    try:
        conn.execute("UPDATE posts SET pinned = ? WHERE id = ?", (1 if pinned else 0, post_id))
        conn.commit()
        return get_post(post_id)
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------

def create_comment(post_id: int, author_name: str, body: str) -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "INSERT INTO comments (post_id, author_name, body) VALUES (?, ?, ?)",
            (post_id, author_name.strip(), body.strip())
        )
        conn.commit()
        row = conn.execute("SELECT * FROM comments WHERE id = ?", (cur.lastrowid,)).fetchone()
        comment = dict(row)
        broadcast({'type': 'new_comment', 'post_id': post_id, 'comment': comment})
        return comment
    finally:
        close_db_connection(conn)


def get_comments(post_id: int) -> list[dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM comments WHERE post_id = ? ORDER BY created_at ASC",
            (post_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


def delete_comment(comment_id: int) -> bool:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT post_id FROM comments WHERE id = ?", (comment_id,)).fetchone()
        conn.execute("DELETE FROM comments WHERE id = ?", (comment_id,))
        conn.commit()
        if row:
            broadcast({'type': 'delete_comment', 'comment_id': comment_id, 'post_id': row['post_id']})
        return True
    except Exception as e:
        logger.error("[Discussion] delete_comment: %s", e)
        return False
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Reactions
# ---------------------------------------------------------------------------

def toggle_reaction(post_id: int, author_name: str, emoji: str) -> dict:
    """Toggle an emoji reaction. Returns updated reaction summary for the post."""
    conn = get_db_connection()
    try:
        existing = conn.execute(
            "SELECT id FROM reactions WHERE post_id = ? AND author_name = ? AND emoji = ?",
            (post_id, author_name, emoji)
        ).fetchone()
        if existing:
            conn.execute("DELETE FROM reactions WHERE id = ?", (existing['id'],))
        else:
            conn.execute(
                "INSERT INTO reactions (post_id, author_name, emoji) VALUES (?, ?, ?)",
                (post_id, author_name, emoji)
            )
        conn.commit()
        summary = _get_reactions_summary(post_id, conn)
        broadcast({'type': 'reaction', 'post_id': post_id, 'reactions': summary})
        return summary
    finally:
        close_db_connection(conn)


def _get_reactions_summary(post_id: int, conn) -> dict:
    """Returns {emoji: count} for a post."""
    rows = conn.execute(
        "SELECT emoji, COUNT(*) as cnt FROM reactions WHERE post_id = ? GROUP BY emoji",
        (post_id,)
    ).fetchall()
    return {r['emoji']: r['cnt'] for r in rows}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_discussion_stats() -> dict:
    conn = get_db_connection()
    try:
        total_posts    = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        total_comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
        pinned         = conn.execute("SELECT COUNT(*) FROM posts WHERE pinned=1").fetchone()[0]
        today          = datetime.utcnow().date().isoformat()
        today_posts    = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE DATE(created_at) = ?", (today,)
        ).fetchone()[0]
        return {
            'total_posts':    total_posts,
            'total_comments': total_comments,
            'pinned_posts':   pinned,
            'posts_today':    today_posts,
        }
    finally:
        close_db_connection(conn)
