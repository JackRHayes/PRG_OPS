"""
Oryon — Job Financials Module
Budget tracking, expense logging, and cost roll-ups per job and company-wide.
"""

import logging
from datetime import date, datetime
from database import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_financial_tables():
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS budgets (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id           TEXT    UNIQUE NOT NULL,
                labor_budget     REAL    DEFAULT 0,
                material_budget  REAL    DEFAULT 0,
                other_budget     REAL    DEFAULT 0,
                total_budget     REAL    DEFAULT 0,
                created_at       TEXT    DEFAULT CURRENT_TIMESTAMP,
                updated_at       TEXT    DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );

            CREATE TABLE IF NOT EXISTS expenses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id      TEXT    NOT NULL,
                category    TEXT    DEFAULT 'Other',
                amount      REAL    NOT NULL,
                description TEXT    DEFAULT '',
                expense_date TEXT   DEFAULT CURRENT_DATE,
                created_by  TEXT    DEFAULT '',
                created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );
        """)
        conn.commit()
        logger.info("[Finance] Tables initialised")
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Budget CRUD
# ---------------------------------------------------------------------------

def set_budget(job_id: str, data: dict) -> dict:
    labor    = float(data.get('labor_budget',    0))
    material = float(data.get('material_budget', 0))
    other    = float(data.get('other_budget',    0))
    total    = float(data.get('total_budget', labor + material + other))
    now      = datetime.utcnow().isoformat()

    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO budgets (job_id, labor_budget, material_budget, other_budget, total_budget, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                labor_budget=excluded.labor_budget,
                material_budget=excluded.material_budget,
                other_budget=excluded.other_budget,
                total_budget=excluded.total_budget,
                updated_at=excluded.updated_at
        """, (job_id, labor, material, other, total, now))
        conn.commit()
        return get_budget(job_id)
    finally:
        close_db_connection(conn)


def get_budget(job_id: str) -> dict:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM budgets WHERE job_id = ?", (job_id,)).fetchone()
        if row:
            return dict(row)
        return {'job_id': job_id, 'labor_budget': 0, 'material_budget': 0,
                'other_budget': 0, 'total_budget': 0}
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Expenses CRUD
# ---------------------------------------------------------------------------

def add_expense(job_id: str, data: dict) -> dict:
    conn = get_db_connection()
    try:
        cur = conn.execute("""
            INSERT INTO expenses (job_id, category, amount, description, expense_date, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            data.get('category', 'Other'),
            float(data.get('amount', 0)),
            data.get('description', '').strip(),
            data.get('expense_date', date.today().isoformat()),
            data.get('created_by', '').strip(),
        ))
        # Roll up to job actual_cost
        conn.execute("""
            UPDATE jobs SET actual_cost = actual_cost + ?, updated_at = ?
            WHERE job_id = ?
        """, (float(data.get('amount', 0)), datetime.utcnow().isoformat(), job_id))
        conn.commit()
        row = conn.execute("SELECT * FROM expenses WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row)
    finally:
        close_db_connection(conn)


def get_expenses(job_id: str) -> list[dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM expenses WHERE job_id = ? ORDER BY expense_date DESC, id DESC",
            (job_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        close_db_connection(conn)


def delete_expense(expense_id: int) -> bool:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT job_id, amount FROM expenses WHERE id = ?", (expense_id,)).fetchone()
        if not row:
            return False
        # Reverse the cost roll-up
        conn.execute("""
            UPDATE jobs SET actual_cost = MAX(0, actual_cost - ?), updated_at = ?
            WHERE job_id = ?
        """, (row['amount'], datetime.utcnow().isoformat(), row['job_id']))
        conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error("[Finance] delete_expense: %s", e)
        return False
    finally:
        close_db_connection(conn)


# ---------------------------------------------------------------------------
# Per-job financial summary
# ---------------------------------------------------------------------------

def get_job_financials(job_id: str) -> dict:
    budget   = get_budget(job_id)
    expenses = get_expenses(job_id)

    conn = get_db_connection()
    try:
        job_row = conn.execute(
            "SELECT actual_cost, labor_cost, material_cost FROM jobs WHERE job_id = ?",
            (job_id,)
        ).fetchone()
    finally:
        close_db_connection(conn)

    actual_cost   = dict(job_row)['actual_cost']   if job_row else 0
    labor_cost    = dict(job_row)['labor_cost']     if job_row else 0
    material_cost = dict(job_row)['material_cost']  if job_row else 0

    # Expense breakdown by category
    by_cat = {}
    for e in expenses:
        by_cat[e['category']] = by_cat.get(e['category'], 0) + e['amount']

    total_budget = budget['total_budget']
    remaining    = total_budget - actual_cost
    utilization  = round((actual_cost / total_budget * 100), 1) if total_budget > 0 else 0
    over_budget  = actual_cost > total_budget and total_budget > 0

    return {
        'job_id':        job_id,
        'budget':        budget,
        'expenses':      expenses,
        'actual_cost':   round(actual_cost, 2),
        'labor_cost':    round(labor_cost, 2),
        'material_cost': round(material_cost, 2),
        'remaining':     round(remaining, 2),
        'utilization':   utilization,
        'over_budget':   over_budget,
        'by_category':   {k: round(v, 2) for k, v in by_cat.items()},
        'profit_loss':   round(remaining, 2),
    }


# ---------------------------------------------------------------------------
# Company-wide overview
# ---------------------------------------------------------------------------

def get_financials_overview() -> dict:
    conn = get_db_connection()
    try:
        jobs = conn.execute("""
            SELECT j.job_id, j.contractor, j.status, j.actual_cost, j.labor_cost, j.material_cost,
                   b.total_budget, b.labor_budget, b.material_budget, b.other_budget
            FROM jobs j
            LEFT JOIN budgets b ON b.job_id = j.job_id
        """).fetchall()
        jobs = [dict(r) for r in jobs]

        total_budget  = sum(j['total_budget']  or 0 for j in jobs)
        total_spent   = sum(j['actual_cost']   or 0 for j in jobs)
        total_labor   = sum(j['labor_cost']    or 0 for j in jobs)
        total_material = sum(j['material_cost'] or 0 for j in jobs)
        over_budget   = [j for j in jobs if (j['total_budget'] or 0) > 0 and (j['actual_cost'] or 0) > (j['total_budget'] or 0)]

        # Top 10 jobs by spend
        top_jobs = sorted(
            [j for j in jobs if (j['actual_cost'] or 0) > 0],
            key=lambda x: x['actual_cost'], reverse=True
        )[:10]

        return {
            'total_budget':   round(total_budget, 2),
            'total_spent':    round(total_spent, 2),
            'total_labor':    round(total_labor, 2),
            'total_material': round(total_material, 2),
            'remaining':      round(total_budget - total_spent, 2),
            'over_budget_count': len(over_budget),
            'over_budget_jobs':  over_budget,
            'top_spending_jobs': top_jobs,
            'utilization':    round(total_spent / total_budget * 100, 1) if total_budget > 0 else 0,
        }
    finally:
        close_db_connection(conn)
