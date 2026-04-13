"""Load EU CN-8 codes into kb_hs_codes.

Source: UK Trade Tariff API (https://www.trade-tariff.service.gov.uk).
The UK Global Tariff retains the EU Combined Nomenclature structure for
chapters 1–97 at the 8-digit level. Used here as a practical proxy for
CN-8 because the EU does not publish a stable machine-readable CSV.

Caveat: ~95% aligned with EU CN. Suitable for RAG/classification, NOT
for customs filings — for that, use the official EU Annex I.

Idempotent: ON CONFLICT DO NOTHING on (code, code_type).
"""

import logging
import os
import time

import httpx
import psycopg2

logger = logging.getLogger(__name__)

DEFAULT_TARIFF_BASE = "https://www.trade-tariff.service.gov.uk/api/v2"

REQUEST_TIMEOUT = 30
INTER_REQUEST_DELAY = 0.3  # seconds — polite throttling
MAX_RETRIES = 5
INITIAL_BACKOFF = 5.0  # seconds — doubles each retry on 429


def get_base_url() -> str:
    return os.environ.get("UK_TARIFF_API_BASE", DEFAULT_TARIFF_BASE)


def _get_with_retry(client: httpx.Client, path: str) -> httpx.Response:
    """GET with exponential backoff on HTTP 429 (rate limit)."""
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = client.get(path, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp
        wait = backoff
        logger.warning(
            "Rate-limited on %s (attempt %d/%d) — sleeping %.1fs",
            path, attempt, MAX_RETRIES, wait,
        )
        time.sleep(wait)
        backoff *= 2
    # Final attempt — let it raise
    resp = client.get(path, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


def fetch_chapter_ids(client: httpx.Client) -> list[str]:
    """Return all chapter IDs (zero-padded 2-digit strings)."""
    resp = _get_with_retry(client, "/chapters")
    chapters = resp.json().get("data", [])
    ids: list[str] = []
    for ch in chapters:
        item_id = ch.get("attributes", {}).get("goods_nomenclature_item_id", "")
        if len(item_id) >= 2 and item_id[:2].isdigit():
            ids.append(item_id[:2])
    return sorted(set(ids))


def fetch_chapter_detail(client: httpx.Client, chapter_id: str) -> dict:
    """Fetch chapter detail (includes its headings)."""
    resp = _get_with_retry(client, f"/chapters/{chapter_id}")
    return resp.json()


def fetch_heading_detail(client: httpx.Client, heading_id: str) -> dict:
    """Fetch heading detail (includes its commodities)."""
    resp = _get_with_retry(client, f"/headings/{heading_id}")
    return resp.json()


def extract_headings(chapter_payload: dict) -> list[dict]:
    """Extract heading rows from a chapter payload."""
    out: list[dict] = []
    for item in chapter_payload.get("included", []):
        if item.get("type") != "heading":
            continue
        a = item.get("attributes", {})
        item_id = a.get("goods_nomenclature_item_id", "")
        desc = a.get("description") or a.get("formatted_description", "")
        if not item_id or not desc:
            continue
        out.append({
            "code": item_id[:4],          # 4-digit heading
            "description": desc.strip(),
        })
    return out


def extract_cn8_codes(heading_payload: dict) -> list[dict]:
    """Extract distinct CN-8 codes from a heading payload.

    UK API returns 10-digit codes; we truncate to 8 and dedupe by description.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for item in heading_payload.get("included", []):
        if item.get("type") != "commodity":
            continue
        a = item.get("attributes", {})
        item_id = a.get("goods_nomenclature_item_id", "")
        # Skip the parent suffix-10 entries (they're the heading itself)
        suffix = a.get("producline_suffix", "")
        if not item_id or len(item_id) < 8:
            continue
        cn8 = item_id[:8]
        # Only take suffix==80 entries (final classifiable commodities)
        if suffix and suffix != "80":
            continue
        if cn8 in seen:
            continue
        desc = a.get("description") or a.get("formatted_description", "")
        if not desc:
            continue
        seen.add(cn8)
        out.append({
            "code": cn8,
            "description": desc.strip(),
            "parent_code": cn8[:4],
        })
    return out


def insert_codes(rows: list[dict], db_conn) -> int:
    """Insert CN-8 rows into kb_hs_codes. Returns inserted count."""
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
                    row.get("parent_code"),
                    row.get("country_scope", "EU"),
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


def load_eu_cn_codes(
    db_conn,
    base_url: str | None = None,
    chapter_filter: list[str] | None = None,
) -> dict:
    """Download CN-8 codes from UK Trade Tariff and load into kb_hs_codes.

    chapter_filter — optional list of 2-digit chapter IDs to limit scope
    (useful for testing). If None, loads all 1–97.
    """
    base = base_url or get_base_url()
    total_headings = 0
    total_codes = 0
    inserted = 0
    failed_headings = 0

    with httpx.Client(base_url=base, timeout=REQUEST_TIMEOUT,
                      follow_redirects=True) as client:
        chapter_ids = fetch_chapter_ids(client)
        if chapter_filter:
            chapter_ids = [c for c in chapter_ids if c in chapter_filter]

        logger.info("Loading CN-8 codes for %d chapters", len(chapter_ids))

        # Insert chapter rows themselves
        for chapter_id in chapter_ids:
            try:
                ch_payload = fetch_chapter_detail(client, chapter_id)
                ch_attrs = ch_payload.get("data", {}).get("attributes", {})
                ch_desc = ch_attrs.get("description") or ch_attrs.get("formatted_description")
                if ch_desc:
                    inserted += insert_codes([{
                        "code": chapter_id,
                        "code_type": "EU_CN_2",
                        "description": ch_desc.strip(),
                        "parent_code": None,
                        "country_scope": "EU",
                    }], db_conn)

                headings = extract_headings(ch_payload)
                total_headings += len(headings)

                # Insert heading rows
                heading_rows = [
                    {
                        "code": h["code"],
                        "code_type": "EU_CN_4",
                        "description": h["description"],
                        "parent_code": chapter_id,
                        "country_scope": "EU",
                    }
                    for h in headings
                ]
                inserted += insert_codes(heading_rows, db_conn)

                # Fetch each heading's commodities
                for h in headings:
                    time.sleep(INTER_REQUEST_DELAY)
                    try:
                        h_payload = fetch_heading_detail(client, h["code"])
                        cn8_rows = extract_cn8_codes(h_payload)
                        total_codes += len(cn8_rows)
                        rows_to_insert = [
                            {
                                "code": r["code"],
                                "code_type": "EU_CN_8",
                                "description": r["description"],
                                "parent_code": r.get("parent_code"),
                                "country_scope": "EU",
                            }
                            for r in cn8_rows
                        ]
                        inserted += insert_codes(rows_to_insert, db_conn)
                    except httpx.HTTPError as exc:
                        failed_headings += 1
                        logger.warning(
                            "Failed to fetch heading %s: %s", h["code"], exc,
                        )

                logger.info(
                    "Chapter %s done: %d headings, %d CN-8 codes so far",
                    chapter_id, len(headings), total_codes,
                )

            except httpx.HTTPError as exc:
                logger.error("Failed to fetch chapter %s: %s", chapter_id, exc)

    logger.info(
        "EU CN load complete: %d headings, %d CN-8 codes, %d new inserted, "
        "%d failed",
        total_headings, total_codes, inserted, failed_headings,
    )
    return {
        "headings": total_headings,
        "cn8_codes": total_codes,
        "inserted": inserted,
        "failed_headings": failed_headings,
    }


def main():
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Load EU CN-8 codes")
    parser.add_argument("--chapters", type=str, default=None,
                        help="Comma-separated chapter IDs, e.g. 84,85 "
                             "(default: all)")
    args = parser.parse_args()

    chapter_filter = None
    if args.chapters:
        chapter_filter = [c.strip().zfill(2) for c in args.chapters.split(",")]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = load_eu_cn_codes(conn, chapter_filter=chapter_filter)
        print(
            f"Headings: {result['headings']}  "
            f"CN-8 codes: {result['cn8_codes']}  "
            f"Inserted: {result['inserted']}  "
            f"Failed: {result['failed_headings']}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
