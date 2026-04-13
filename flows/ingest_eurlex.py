"""Prefect flow: standalone EUR-Lex ingestion for isolated testing.

Ingests from EUR-Lex, extracts, processes, and loads.
Does not classify HS codes — use the full pipeline for that.
"""

import logging
import os
from datetime import datetime, timezone

import psycopg2
from prefect import flow, task

from pipeline.llm_client import LLMClient
from pipeline.sources.eurlex import ingest_new_documents as eurlex_ingest
from pipeline.extract import extract
from pipeline.process import process
from pipeline.validate import validate_and_route
from pipeline.load import load

logger = logging.getLogger(__name__)


@task(name="eurlex-ingest")
def fetch_eurlex(since: datetime) -> list[dict]:
    """Fetch new EUR-Lex documents."""
    return eurlex_ingest(since)


@task(name="eurlex-extract")
def extract_doc(raw_doc: dict, db_conn, llm: LLMClient) -> dict:
    """Extract regulation metadata from a single document."""
    return extract(raw_doc, db_conn, llm)


@task(name="eurlex-process")
def process_doc(record: dict, llm: LLMClient) -> dict:
    """Clean and enrich a single record."""
    return process(record, llm)


@task(name="eurlex-validate-load")
def validate_and_load(record: dict, db_conn) -> str:
    """Validate and load a single record."""
    route = validate_and_route(record, db_conn)
    if route == "pass":
        load(record, db_conn)
    return route


@flow(name="ingest-eurlex")
def ingest_eurlex_flow(since: datetime | None = None) -> dict:
    """Standalone EUR-Lex ingestion flow.

    Parameters
    ----------
    since : datetime | None
        Fetch documents modified after this datetime.
        Defaults to 30 days ago.
    """
    if since is None:
        since = datetime(2020, 1, 1, tzinfo=timezone.utc)

    llm = LLMClient()
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False

    try:
        raw_docs = fetch_eurlex(since)
        loaded = 0
        quarantined = 0
        failed = 0

        for doc in raw_docs:
            try:
                extracted = extract_doc(doc, conn, llm)
                processed = process_doc(extracted, llm)

                if processed.get("_skip"):
                    continue

                route = validate_and_load(processed, conn)
                if route == "pass":
                    loaded += 1
                elif route == "quarantine":
                    quarantined += 1
            except Exception as exc:
                logger.error(
                    "Failed to process EUR-Lex doc %s: %s",
                    doc.get("document_id", "unknown"),
                    exc,
                )
                failed += 1

        summary = {
            "source": "EUR-Lex",
            "ingested": len(raw_docs),
            "loaded": loaded,
            "quarantined": quarantined,
            "failed": failed,
        }
        logger.info("EUR-Lex ingest complete: %s", summary)
        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    ingest_eurlex_flow()
