"""Load EU harmonised standards from a directory of per-directive XLSX files.

Each XLSX is the official "Summary list" published by the EU Commission for
one directive (e.g. ``2006_42_EC.xlsx`` for the Machinery Directive). One
row per (standard, directive) pair; the same standard may appear under
multiple directives via separate XLSX files.

Confidence is set to 0.92 — Tier 3 official EU portal, deterministic XLSX
extraction (no LLM), well above the 0.90 auto-accept threshold.

Usage:
    python -m kb.load_harmonised_standards data/harmonised_standards
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from kb.sources.harmonised_standards_xlsx import parse_xlsx

logger = logging.getLogger(__name__)

CONFIDENCE = 0.92
EXTRACTION_METHOD = "oj_xlsx"
SOURCE_BASE = (
    "https://single-market-economy.ec.europa.eu/single-market/goods/"
    "european-standards/harmonised-standards"
)


def _upsert_standard(conn, rec: dict, now: datetime) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO kb_harmonised_standards
            (standard_code, title, eso, directive_ref,
             in_force_from, withdrawn_on,
             oj_publication_ref, oj_withdrawal_ref,
             source_url, confidence, last_verified_at, extraction_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (standard_code, directive_ref) DO UPDATE SET
            title = COALESCE(EXCLUDED.title, kb_harmonised_standards.title),
            eso = COALESCE(EXCLUDED.eso, kb_harmonised_standards.eso),
            in_force_from = COALESCE(EXCLUDED.in_force_from, kb_harmonised_standards.in_force_from),
            withdrawn_on = EXCLUDED.withdrawn_on,
            oj_publication_ref = COALESCE(EXCLUDED.oj_publication_ref, kb_harmonised_standards.oj_publication_ref),
            oj_withdrawal_ref = COALESCE(EXCLUDED.oj_withdrawal_ref, kb_harmonised_standards.oj_withdrawal_ref),
            source_url = EXCLUDED.source_url,
            confidence = EXCLUDED.confidence,
            last_verified_at = EXCLUDED.last_verified_at,
            extraction_method = EXCLUDED.extraction_method
        """,
        (
            rec["standard_code"],
            rec.get("title"),
            rec.get("eso"),
            rec["directive_ref"],
            rec.get("in_force_from"),
            rec.get("withdrawn_on"),
            rec.get("oj_publication_ref"),
            rec.get("oj_withdrawal_ref"),
            SOURCE_BASE,
            CONFIDENCE,
            now,
            EXTRACTION_METHOD,
        ),
    )
    cur.close()


def load_directory(xlsx_dir: Path, conn) -> dict:
    now = datetime.now(timezone.utc)
    files = sorted(xlsx_dir.glob("*.xlsx"))
    parsed_files = 0
    rows_inserted = 0
    skipped: list[tuple[str, str]] = []
    by_directive: dict[str, int] = {}

    for path in files:
        try:
            recs = parse_xlsx(path)
        except Exception as exc:
            skipped.append((path.name, f"parse-error: {exc}"))
            continue
        if not recs:
            skipped.append((path.name, "no rows"))
            continue

        try:
            for rec in recs:
                _upsert_standard(conn, rec, now)
                by_directive[rec["directive_ref"]] = by_directive.get(rec["directive_ref"], 0) + 1
                rows_inserted += 1
            conn.commit()
            parsed_files += 1
        except Exception as exc:
            conn.rollback()
            skipped.append((path.name, f"db-error: {exc}"))

    return {
        "files_seen": len(files),
        "files_parsed": parsed_files,
        "rows_inserted": rows_inserted,
        "by_directive": by_directive,
        "skipped": skipped,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx_dir", type=Path)
    args = ap.parse_args()

    if not args.xlsx_dir.is_dir():
        print(f"Not a directory: {args.xlsx_dir}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = load_directory(args.xlsx_dir, conn)
    finally:
        conn.close()

    print(f"Files seen:    {result['files_seen']}")
    print(f"Files parsed:  {result['files_parsed']}")
    print(f"Rows upserted: {result['rows_inserted']}")
    print("By directive:")
    for d, n in sorted(result["by_directive"].items()):
        print(f"  {d:14s} {n}")
    if result["skipped"]:
        print(f"Skipped: {len(result['skipped'])}")
        for name, reason in result["skipped"]:
            print(f"  - {name}: {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
