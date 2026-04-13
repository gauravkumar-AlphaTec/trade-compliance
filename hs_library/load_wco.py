"""Load WCO 6-digit Harmonized System codes into kb_hs_codes.

Source: https://github.com/datasets/harmonized-system (MIT-licensed,
maintained mirror of the WCO HS nomenclature).

Idempotent: uses ON CONFLICT DO NOTHING on the (code, code_type) UNIQUE
constraint so reruns are safe.
"""

import csv
import io
import logging
import os
from typing import Iterable

import httpx
import psycopg2

logger = logging.getLogger(__name__)

DEFAULT_WCO_CSV_URL = (
    "https://raw.githubusercontent.com/datasets/harmonized-system/"
    "master/data/harmonized-system.csv"
)


def get_csv_url() -> str:
    return os.environ.get("WCO_HS_CSV_URL", DEFAULT_WCO_CSV_URL)


def fetch_csv(url: str | None = None) -> str:
    """Download the WCO HS CSV. Returns the raw CSV text."""
    target = url or get_csv_url()
    logger.info("Downloading WCO HS CSV from %s", target)
    response = httpx.get(target, timeout=60, follow_redirects=True)
    response.raise_for_status()
    return response.text


def parse_csv(csv_text: str) -> list[dict]:
    """Parse the WCO CSV into rows ready for kb_hs_codes.

    Source schema: section, hscode, description, parent, level
    Levels: 2 (chapter), 4 (heading), 6 (subheading)

    Only level==6 entries are imported as WCO_6 codes; chapters and
    headings are kept as parent links but not as classifiable codes.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    rows: list[dict] = []
    for entry in reader:
        try:
            level = int(entry.get("level", "") or 0)
        except ValueError:
            continue

        code = (entry.get("hscode") or "").strip()
        description = (entry.get("description") or "").strip()
        if not code or not description:
            continue

        parent = (entry.get("parent") or "").strip() or None

        rows.append({
            "code": code,
            "code_type": "WCO_6" if level == 6 else f"WCO_{level}",
            "description": description,
            "parent_code": parent,
            "country_scope": "WCO",
            "level": level,
        })
    return rows


def insert_rows(rows: Iterable[dict], db_conn) -> int:
    """Insert HS code rows into kb_hs_codes. Returns count actually inserted."""
    inserted = 0
    cur = db_conn.cursor()
    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO kb_hs_codes
                    (code, code_type, description, parent_code, country_scope)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (code, code_type) DO NOTHING
                """,
                (
                    row["code"],
                    row["code_type"],
                    row["description"],
                    row["parent_code"],
                    row["country_scope"],
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return inserted


def load_wco_codes(db_conn, url: str | None = None) -> dict:
    """Download, parse, and insert WCO HS codes.

    Returns {"fetched": int, "inserted": int}.
    """
    csv_text = fetch_csv(url)
    rows = parse_csv(csv_text)
    inserted = insert_rows(rows, db_conn)

    logger.info(
        "WCO load complete: %d rows fetched, %d new codes inserted",
        len(rows),
        inserted,
    )
    return {"fetched": len(rows), "inserted": inserted}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = load_wco_codes(conn)
        print(f"Fetched: {result['fetched']}  Inserted: {result['inserted']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
