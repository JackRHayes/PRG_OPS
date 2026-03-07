"""
migrate_data.py — One-time (re-runnable) script to populate oryon.db from CSV files.

Usage:
    python3.11 migrate_data.py
"""

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

from database import init_database, migrate_csv_to_db, get_all_jobs

if __name__ == '__main__':
    print("\n── Oryon Database Migration ──────────────────")
    print("Initialising database schema...")
    init_database()

    print("Migrating CSV data...")
    counts = migrate_csv_to_db()

    print(f"\n  Jobs inserted:    {counts['jobs']}")
    print(f"  RFIs inserted:    {counts['rfis']}")
    print(f"  Permits inserted: {counts['permits']}")

    total_jobs = len(get_all_jobs())
    print(f"\n  Total jobs in DB: {total_jobs}")
    print("──────────────────────────────────────────────")
    print("Migration complete. oryon.db is ready.\n")
