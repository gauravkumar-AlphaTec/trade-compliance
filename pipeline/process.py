"""Processing pipeline: clean, enrich, and batch-process regulation records.

All DB queries use parameterised statements. No f-string SQL.
"""

import hashlib
import logging
import re

import arrow

from pipeline.llm_client import LLMClient

logger = logging.getLogger(__name__)

# Date fields to normalise
DATE_FIELDS = ("effective_date", "expiry_date", "publication_date")


# ------------------------------------------------------------------
# clean
# ------------------------------------------------------------------

def clean(record: dict, db_conn=None) -> dict:
    """Normalise a regulation record before enrichment.

    1. Strip whitespace from all string fields.
    2. Normalise date fields to ISO 8601 (YYYY-MM-DD).
    3. Compute content_hash from full_text.
    4. Deduplicate: if (source_name, document_id, content_hash) already
       exists in the sources table, set record['_skip'] = True.

    Parameters
    ----------
    record : dict
        Raw regulation record from extraction stage.
    db_conn : optional
        Database connection for dedup check. If None, dedup is skipped.

    Returns the mutated record.
    """
    # 1. Strip whitespace from string fields
    for key, value in record.items():
        if isinstance(value, str):
            record[key] = value.strip()

    # 2. Normalise dates
    for field in DATE_FIELDS:
        raw_date = record.get(field)
        if raw_date:
            record[field] = _normalise_date(raw_date)

    # 3. Compute content hash
    full_text = record.get("full_text", "")
    if full_text:
        record["content_hash"] = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
    else:
        record["content_hash"] = None

    # 4. Deduplicate
    if db_conn is not None:
        source_name = record.get("source_name", "")
        document_id = record.get("document_id", "")
        content_hash = record.get("content_hash")

        if source_name and document_id and _is_duplicate(
            source_name, document_id, content_hash, db_conn,
        ):
            record["_skip"] = True
            logger.info(
                "Skipping duplicate: source_name=%s document_id=%s",
                source_name,
                document_id,
            )

    return record


# ------------------------------------------------------------------
# enrich
# ------------------------------------------------------------------

def enrich(record: dict, llm: LLMClient) -> dict:
    """Add LLM-generated summary to a regulation record.

    Adds 'summary' field via LLMClient.generate_summary().
    Only generates summary if full_text is present and summary is missing.

    Returns the mutated record.
    """
    full_text = record.get("full_text", "")

    # Generate summary if we have text and no existing summary
    if full_text and not record.get("summary"):
        try:
            record["summary"] = llm.generate_summary(full_text, max_words=150)
        except Exception as exc:
            logger.warning(
                "Summary generation failed for %s: %s",
                record.get("document_id", "unknown"),
                exc,
            )

    return record


# ------------------------------------------------------------------
# process
# ------------------------------------------------------------------

def process(record: dict, llm: LLMClient, db_conn=None) -> dict:
    """Run clean -> enrich on a single record.

    If record has _skip=True after clean, returns as-is (no enrichment).
    """
    record = clean(record, db_conn=db_conn)

    if record.get("_skip"):
        return record

    record = enrich(record, llm)
    return record


# ------------------------------------------------------------------
# process_batch
# ------------------------------------------------------------------

def process_batch(
    records: list[dict],
    llm: LLMClient,
    db_conn=None,
) -> tuple[list[dict], list[dict]]:
    """Process a batch of records. Per-record error handling.

    Returns (processed_records, failed_records).
    One failure does not abort the batch.

    Each failed record is wrapped as:
        {"record": original_record, "error": str(exception)}
    """
    processed: list[dict] = []
    failed: list[dict] = []

    for record in records:
        try:
            result = process(record, llm, db_conn=db_conn)
            processed.append(result)
        except Exception as exc:
            doc_id = record.get("document_id", "unknown") if isinstance(record, dict) else "unknown"
            logger.error(
                "Processing failed for document_id=%s: %s",
                doc_id,
                exc,
            )
            failed.append({"record": record, "error": str(exc)})

    logger.info(
        "Batch complete: %d processed, %d failed",
        len(processed),
        len(failed),
    )
    return processed, failed


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _normalise_date(raw: str) -> str | None:
    """Parse a date string and return ISO 8601 (YYYY-MM-DD).

    Uses the arrow library for flexible parsing. Returns None if
    the date cannot be parsed.
    """
    if not raw or not isinstance(raw, str):
        return None

    raw = raw.strip()
    if not raw:
        return None

    try:
        parsed = arrow.get(raw)
        return parsed.format("YYYY-MM-DD")
    except (arrow.parser.ParserError, ValueError):
        pass

    # Try common formats explicitly
    for fmt in (
        "YYYY-MM-DD",
        "MM/DD/YYYY",
        "DD/MM/YYYY",
        "MMMM D, YYYY",
        "D MMMM YYYY",
        "YYYY-MM-DDTHH:mm:ssZ",
        "YYYY-MM-DDTHH:mm:ss",
    ):
        try:
            parsed = arrow.get(raw, fmt)
            return parsed.format("YYYY-MM-DD")
        except (arrow.parser.ParserError, ValueError):
            continue

    logger.warning("Could not parse date: %s", raw)
    return None


def _is_duplicate(
    source_name: str,
    document_id: str,
    content_hash: str | None,
    db_conn,
) -> bool:
    """Check if a record with matching source_name, document_id, and
    content_hash already exists in the sources table.

    If content_hash is None, checks only source_name + document_id.
    """
    cur = db_conn.cursor()
    try:
        if content_hash:
            cur.execute(
                """
                SELECT 1 FROM sources
                WHERE source_name = %s AND document_id = %s AND content_hash = %s
                LIMIT 1
                """,
                (source_name, document_id, content_hash),
            )
        else:
            cur.execute(
                """
                SELECT 1 FROM sources
                WHERE source_name = %s AND document_id = %s
                LIMIT 1
                """,
                (source_name, document_id),
            )
        return cur.fetchone() is not None
    finally:
        cur.close()
