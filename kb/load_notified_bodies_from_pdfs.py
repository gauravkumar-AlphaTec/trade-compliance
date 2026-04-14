"""Load EU notified bodies for Germany from a directory of NANDO/SMCS PDFs.

Each PDF is one designation: a (notified body × directive) pair. The same
body recurs across multiple PDFs (one per directive it is notified for),
so we upsert on `nb_number` for the body row and on `(nb_id, directive_ref)`
for the per-designation row.

Confidence is set to 0.92 — Tier 3 government portal (EU Commission SMCS),
with structured-template extraction (no LLM), well above the 0.90 auto-accept
threshold.

Usage:
    python -m kb.load_notified_bodies_from_pdfs data/Europa\\ docs\\ Germany --country DE
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from kb.sources.nando_pdf import extract_notification

logger = logging.getLogger(__name__)

CONFIDENCE = 0.92
EXTRACTION_METHOD = "nando_pdf"
NANDO_BASE = "https://webgate.ec.europa.eu/single-market-compliance-space/notified-bodies"


def _country_id(conn, iso2: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM kb_country_profiles WHERE iso2 = %s", (iso2,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError(f"No kb_country_profiles row for {iso2}")
    return row[0]


def _upsert_body(conn, country_id: int, rec: dict, now: datetime) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO kb_notified_bodies
            (nb_number, name, country_id, city, email, website,
             accreditation_body, status, source_url, confidence,
             last_verified_at, extraction_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, %s, %s)
        ON CONFLICT (nb_number) DO UPDATE SET
            name = EXCLUDED.name,
            country_id = EXCLUDED.country_id,
            city = COALESCE(EXCLUDED.city, kb_notified_bodies.city),
            email = COALESCE(EXCLUDED.email, kb_notified_bodies.email),
            website = COALESCE(EXCLUDED.website, kb_notified_bodies.website),
            accreditation_body = COALESCE(EXCLUDED.accreditation_body, kb_notified_bodies.accreditation_body),
            source_url = EXCLUDED.source_url,
            confidence = EXCLUDED.confidence,
            last_verified_at = EXCLUDED.last_verified_at,
            extraction_method = EXCLUDED.extraction_method
        RETURNING id
        """,
        (
            rec["nb_number"],
            rec["name"],
            country_id,
            rec.get("city"),
            rec.get("email"),
            rec.get("website"),
            rec.get("accreditation_body"),
            NANDO_BASE,
            CONFIDENCE,
            now,
            EXTRACTION_METHOD,
        ),
    )
    nb_id = cur.fetchone()[0]
    cur.close()
    return nb_id


def _upsert_designation(conn, nb_id: int, rec: dict, now: datetime) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO kb_notified_body_directives
            (nb_id, directive_ref, directive_name, notifying_authority,
             last_approval_date, assessment_standards, source_url,
             confidence, last_verified_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (nb_id, directive_ref) DO UPDATE SET
            directive_name = EXCLUDED.directive_name,
            notifying_authority = COALESCE(EXCLUDED.notifying_authority, kb_notified_body_directives.notifying_authority),
            last_approval_date = COALESCE(EXCLUDED.last_approval_date, kb_notified_body_directives.last_approval_date),
            assessment_standards = CASE
                WHEN COALESCE(array_length(EXCLUDED.assessment_standards, 1), 0) > 0
                THEN EXCLUDED.assessment_standards
                ELSE kb_notified_body_directives.assessment_standards
            END,
            source_url = EXCLUDED.source_url,
            confidence = EXCLUDED.confidence,
            last_verified_at = EXCLUDED.last_verified_at
        """,
        (
            nb_id,
            rec["directive_ref"],
            rec.get("directive_name"),
            rec.get("notifying_authority"),
            rec.get("last_approval_date"),
            rec.get("assessment_standards") or [],
            NANDO_BASE,
            CONFIDENCE,
            now,
        ),
    )
    cur.close()


def load_directory(pdf_dir: Path, iso2: str, conn) -> dict:
    country_id = _country_id(conn, iso2)
    now = datetime.now(timezone.utc)

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    parsed = 0
    skipped: list[tuple[str, str]] = []
    bodies: set[str] = set()
    designations = 0

    for path in pdfs:
        try:
            rec = extract_notification(path)
        except Exception as exc:
            skipped.append((path.name, f"parse-error: {exc}"))
            continue
        if not rec:
            skipped.append((path.name, "empty (no NB number)"))
            continue
        if not rec.get("directive_ref"):
            skipped.append((path.name, "no directive_ref in body"))
            continue

        try:
            nb_id = _upsert_body(conn, country_id, rec, now)
            _upsert_designation(conn, nb_id, rec, now)
            conn.commit()
            bodies.add(rec["nb_number"])
            designations += 1
            parsed += 1
        except Exception as exc:
            conn.rollback()
            skipped.append((path.name, f"db-error: {exc}"))

    return {
        "pdfs_seen": len(pdfs),
        "parsed": parsed,
        "unique_bodies": len(bodies),
        "designations": designations,
        "skipped": skipped,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_dir", type=Path)
    ap.add_argument("--country", default="DE")
    args = ap.parse_args()

    if not args.pdf_dir.is_dir():
        print(f"Not a directory: {args.pdf_dir}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = load_directory(args.pdf_dir, args.country, conn)
    finally:
        conn.close()

    print(f"PDFs seen:       {result['pdfs_seen']}")
    print(f"Parsed OK:       {result['parsed']}")
    print(f"Unique bodies:   {result['unique_bodies']}")
    print(f"Designations:    {result['designations']}")
    print(f"Skipped:         {len(result['skipped'])}")
    for name, reason in result["skipped"][:20]:
        print(f"  - {name}: {reason}")
    if len(result["skipped"]) > 20:
        print(f"  ... and {len(result['skipped']) - 20} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
