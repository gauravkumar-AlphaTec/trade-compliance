"""Load stage: upsert regulations, sources, and HS mappings into the database.

All DB writes use parameterised queries.  No f-string SQL.
The load() function runs as a single transaction with full rollback on failure.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# upsert_source
# ------------------------------------------------------------------

def upsert_source(source_data: dict, db_conn) -> int:
    """Upsert into the sources table.  Returns the source id.

    Conflict target: (source_name, document_id).
    On conflict, updates url, fetched_at, and content_hash.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO sources
                (source_name, document_id, url, fetched_at, content_hash)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_name, document_id) DO UPDATE SET
                url = EXCLUDED.url,
                fetched_at = EXCLUDED.fetched_at,
                content_hash = EXCLUDED.content_hash
            RETURNING id
            """,
            (
                source_data.get("source_name", ""),
                source_data.get("document_id", ""),
                source_data.get("url"),
                source_data.get("fetched_at", datetime.now(timezone.utc)),
                source_data.get("content_hash"),
            ),
        )
        row = cur.fetchone()
        return row[0]
    finally:
        cur.close()


# ------------------------------------------------------------------
# upsert_regulation
# ------------------------------------------------------------------

def upsert_regulation(regulation: dict, source_id: int, db_conn) -> int:
    """Upsert into the regulations table.  Returns the regulation id.

    Uses source_id to link to the sources table.  Updates all fields
    when the record already exists (matched via source_id).
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO regulations
                (source_id, title, document_type, authority, country,
                 effective_date, expiry_date, full_text, summary, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET
                title = EXCLUDED.title,
                document_type = EXCLUDED.document_type,
                authority = EXCLUDED.authority,
                country = EXCLUDED.country,
                effective_date = EXCLUDED.effective_date,
                expiry_date = EXCLUDED.expiry_date,
                full_text = EXCLUDED.full_text,
                summary = EXCLUDED.summary,
                status = EXCLUDED.status,
                updated_at = NOW()
            RETURNING id
            """,
            (
                source_id,
                regulation.get("title", ""),
                regulation.get("document_type"),
                regulation.get("authority"),
                regulation.get("country", ""),
                regulation.get("effective_date"),
                regulation.get("expiry_date"),
                regulation.get("full_text"),
                regulation.get("summary"),
                regulation.get("status", "active"),
            ),
        )
        row = cur.fetchone()
        return row[0]
    finally:
        cur.close()


# ------------------------------------------------------------------
# save_hs_mappings
# ------------------------------------------------------------------

def save_hs_mappings(
    regulation_id: int,
    mappings: list[dict],
    db_conn,
) -> int:
    """Save HS code mappings for a regulation.

    Inserts into regulation_hs_codes.  Skips mappings that have no
    valid hs_code_id.  ON CONFLICT DO UPDATE to refresh confidence.

    Returns the number of mappings saved.
    """
    if not mappings:
        return 0

    saved = 0
    cur = db_conn.cursor()
    try:
        for m in mappings:
            hs_code_id = m.get("hs_code_id")
            if not hs_code_id:
                continue

            cur.execute(
                """
                INSERT INTO regulation_hs_codes
                    (regulation_id, hs_code_id, confidence,
                     mapping_method, reviewed)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (regulation_id, hs_code_id) DO UPDATE SET
                    confidence = EXCLUDED.confidence,
                    mapping_method = EXCLUDED.mapping_method,
                    mapped_at = NOW()
                """,
                (
                    regulation_id,
                    hs_code_id,
                    m.get("confidence", 0.0),
                    m.get("mapping_method", "llm_rag"),
                    m.get("reviewed", False),
                ),
            )
            saved += 1
    finally:
        cur.close()

    return saved


# ------------------------------------------------------------------
# load
# ------------------------------------------------------------------

def load(record: dict, db_conn) -> dict:
    """Load a processed regulation record into the database.

    Runs upsert_source -> upsert_regulation -> save_hs_mappings
    as a single transaction.  Full rollback on any failure.

    Parameters
    ----------
    record : dict
        Processed regulation record with source and mapping data.
    db_conn :
        Database connection (autocommit must be False).

    Returns
    -------
    dict
        {"source_id": int, "regulation_id": int, "mappings_saved": int}
    """
    try:
        # 1. Upsert source provenance
        source_data = {
            "source_name": record.get("source_name", ""),
            "document_id": record.get("document_id", ""),
            "url": record.get("source_url"),
            "fetched_at": record.get("fetched_at", datetime.now(timezone.utc)),
            "content_hash": record.get("content_hash"),
        }
        source_id = upsert_source(source_data, db_conn)

        # 2. Upsert regulation
        regulation_id = upsert_regulation(record, source_id, db_conn)

        # 3. Save HS code mappings
        mappings = record.get("hs_mappings", [])
        mappings_saved = save_hs_mappings(regulation_id, mappings, db_conn)

        # Commit the whole transaction
        db_conn.commit()

        logger.info(
            "Loaded regulation %d (source %d): %d HS mappings saved",
            regulation_id,
            source_id,
            mappings_saved,
        )

        return {
            "source_id": source_id,
            "regulation_id": regulation_id,
            "mappings_saved": mappings_saved,
        }

    except Exception:
        db_conn.rollback()
        raise
