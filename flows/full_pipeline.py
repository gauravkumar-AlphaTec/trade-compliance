"""Prefect flow: full trade compliance regulation pipeline.

Ingests from EUR-Lex and Federal Register, extracts, processes,
classifies HS codes, validates, and loads into the database.

Schedule: weekly (regulations change slowly).
"""

import logging
import os
from datetime import datetime, timezone

import psycopg2
from prefect import flow, task

from pipeline.llm_client import LLMClient
from pipeline.sources.eurlex import ingest_new_documents as eurlex_ingest
from pipeline.sources.federal_register import ingest_new_documents as fr_ingest
from pipeline.extract import extract
from pipeline.process import process, process_batch
from pipeline.hs_classifier import classify_regulation
from pipeline.validate import validate_and_route
from pipeline.load import load

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Pipeline state helpers
# ------------------------------------------------------------------

def get_last_run(stage: str, db_conn) -> datetime:
    """Read the last_run timestamp for a pipeline stage."""
    cur = db_conn.cursor()
    try:
        cur.execute(
            "SELECT last_run FROM pipeline_state WHERE stage = %s",
            (stage,),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if row and row[0]:
        ts = row[0]
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts

    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def update_last_run(stage: str, db_conn) -> None:
    """Update the last_run timestamp for a pipeline stage to now."""
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO pipeline_state (stage, last_run)
            VALUES (%s, NOW())
            ON CONFLICT (stage) DO UPDATE SET last_run = NOW()
            """,
            (stage,),
        )
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()


# ------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------

@task(name="ingest-eurlex")
def ingest_eurlex(since: datetime) -> list[dict]:
    """Ingest new documents from EUR-Lex."""
    docs = eurlex_ingest(since)
    logger.info("Ingested %d documents from EUR-Lex", len(docs))
    return docs


@task(name="ingest-us")
def ingest_us(since: datetime) -> list[dict]:
    """Ingest new documents from the Federal Register."""
    docs = fr_ingest(since)
    logger.info("Ingested %d documents from Federal Register", len(docs))
    return docs


@task(name="extract-all")
def extract_all(raw_docs: list[dict], db_conn, llm: LLMClient) -> list[dict]:
    """Extract regulation metadata from all raw documents."""
    extracted = []
    for doc in raw_docs:
        try:
            result = extract(doc, db_conn, llm)
            extracted.append(result)
        except Exception as exc:
            logger.error(
                "Extraction failed for %s: %s",
                doc.get("document_id", "unknown"),
                exc,
            )
    logger.info("Extracted %d / %d documents", len(extracted), len(raw_docs))
    return extracted


@task(name="process-all")
def process_all(extracted: list[dict], llm: LLMClient) -> list[dict]:
    """Clean and enrich all extracted records."""
    processed, failed = process_batch(extracted, llm)
    logger.info(
        "Processed %d records, %d failed",
        len(processed),
        len(failed),
    )
    return processed


@task(name="classify-all")
def classify_all(
    processed: list[dict],
    db_conn,
    llm: LLMClient,
) -> list[dict]:
    """Classify HS codes for all processed records."""
    for record in processed:
        try:
            mappings = classify_regulation(record, db_conn, llm)
            record["hs_mappings"] = mappings
        except Exception as exc:
            logger.error(
                "HS classification failed for %s: %s",
                record.get("document_id", "unknown"),
                exc,
            )
            record["hs_mappings"] = []
    logger.info("Classified %d records", len(processed))
    return processed


@task(name="validate-all")
def validate_all(classified: list[dict], db_conn) -> list[dict]:
    """Validate all classified records and route to pass/quarantine/duplicate."""
    passed = []
    quarantined = 0
    duplicates = 0

    for record in classified:
        if record.get("_skip"):
            duplicates += 1
            continue

        route = validate_and_route(record, db_conn)
        if route == "pass":
            passed.append(record)
        elif route == "quarantine":
            quarantined += 1
        elif route == "duplicate":
            duplicates += 1

    logger.info(
        "Validation: %d passed, %d quarantined, %d duplicates",
        len(passed),
        quarantined,
        duplicates,
    )
    return passed


@task(name="load-all")
def load_all(validated: list[dict], db_conn) -> int:
    """Load all validated records into the database."""
    loaded = 0
    for record in validated:
        try:
            load(record, db_conn)
            loaded += 1
        except Exception as exc:
            logger.error(
                "Load failed for %s: %s",
                record.get("document_id", "unknown"),
                exc,
            )
    logger.info("Loaded %d / %d validated records", loaded, len(validated))
    return loaded


# ------------------------------------------------------------------
# Flow
# ------------------------------------------------------------------

@flow(name="trade-compliance-pipeline")
def trade_compliance_pipeline() -> dict:
    """Full trade compliance regulation pipeline.

    1. Read LAST_RUN from pipeline_state.
    2. Ingest from EUR-Lex and Federal Register.
    3. Extract -> Process -> Classify -> Validate -> Load.
    4. Log per-stage counts.
    5. Update LAST_RUN on success.

    Schedule: weekly.
    """
    llm = LLMClient()
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False

    try:
        # 1. Read last run
        since = get_last_run("full_pipeline", conn)
        logger.info("Pipeline starting — processing since %s", since)

        # 2. Ingest
        eurlex_docs = ingest_eurlex(since)
        us_docs = ingest_us(since)
        all_raw = eurlex_docs + us_docs

        # 3. Extract
        extracted = extract_all(all_raw, conn, llm)

        # 4. Process (clean + enrich)
        processed = process_all(extracted, llm)

        # 5. Classify HS codes
        classified = classify_all(processed, conn, llm)

        # 6. Validate and route
        validated = validate_all(classified, conn)

        # 7. Load
        loaded_count = load_all(validated, conn)

        # 8. Update last run
        update_last_run("full_pipeline", conn)

        # Summary
        summary = {
            "ingested": len(all_raw),
            "extracted": len(extracted),
            "processed": len(processed),
            "classified": len(classified),
            "validated": len(validated),
            "loaded": loaded_count,
            "quarantined": len(classified) - len(validated),
        }

        logger.info(
            "Pipeline complete: ingested=%d extracted=%d processed=%d "
            "classified=%d validated=%d loaded=%d quarantined=%d",
            summary["ingested"],
            summary["extracted"],
            summary["processed"],
            summary["classified"],
            summary["validated"],
            summary["loaded"],
            summary["quarantined"],
        )

        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    trade_compliance_pipeline()
