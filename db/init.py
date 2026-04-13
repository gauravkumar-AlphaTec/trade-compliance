"""Initialise the trade-compliance database from schema.sql."""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def run() -> None:
    # Load .env from the project root if present
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(schema_sql)

    # Report which tables exist after init
    cur.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    tables = [row[0] for row in cur.fetchall()]
    for table in tables:
        print(f"  ✓ {table}")
    print(f"\nDatabase initialised — {len(tables)} tables ready.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
