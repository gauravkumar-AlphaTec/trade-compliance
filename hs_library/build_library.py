"""Build the HS code mapping library.

Searches kb_hs_codes via GIN full-text index, then uses LLMClient to
classify products against candidate codes.  Results go into
kb_product_hs_mappings for manual review.

All DB writes use parameterised queries.
"""

import logging
import os

import psycopg2

from pipeline.llm_client import LLMClient

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# build_initial_hs_mappings
# ------------------------------------------------------------------

def build_initial_hs_mappings(
    product_categories: list[str],
    country_scope: str,
    db_conn,
    llm: LLMClient,
) -> dict:
    """Build initial HS code mappings for a list of product categories.

    For each category:
      1. GIN full-text search kb_hs_codes.description using plainto_tsquery
         for the 20 most relevant codes matching the country_scope.
      2. Pass category + candidates to LLMClient.classify_hs_code().
      3. INSERT into kb_product_hs_mappings with source='opus_initial',
         verified=False.

    Parameters
    ----------
    product_categories : list[str]
        Product names/categories to classify.
    country_scope : str
        Country scope filter (e.g. 'EU', 'US', 'WCO').
    db_conn :
        Database connection.
    llm : LLMClient
        LLM client for classification.

    Returns
    -------
    dict
        {"mapped": int, "failed": int, "skipped": int}
    """
    mapped = 0
    failed = 0
    skipped = 0

    for category in product_categories:
        category = category.strip()
        if not category:
            skipped += 1
            continue

        try:
            candidates = _search_candidates(category, country_scope, db_conn)

            if not candidates:
                logger.warning(
                    "No HS code candidates found for '%s' (scope=%s)",
                    category,
                    country_scope,
                )
                skipped += 1
                continue

            classification = llm.classify_hs_code(category, candidates)

            _insert_mapping(category, classification, country_scope, db_conn)

            logger.info(
                "Mapped '%s' -> %s (confidence=%.3f, reasoning=%s)",
                category,
                classification.get("code"),
                classification.get("confidence", 0),
                classification.get("reasoning", ""),
            )
            mapped += 1

        except Exception as exc:
            logger.error(
                "Failed to map '%s': %s",
                category,
                exc,
            )
            failed += 1

    logger.info(
        "HS mapping complete: %d mapped, %d failed, %d skipped",
        mapped,
        failed,
        skipped,
    )
    return {"mapped": mapped, "failed": failed, "skipped": skipped}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _search_candidates(
    category: str,
    country_scope: str,
    db_conn,
) -> list[dict]:
    """GIN full-text search against kb_hs_codes.description.

    Returns up to 20 candidate codes ranked by relevance.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, code, code_type, description, country_scope
            FROM kb_hs_codes
            WHERE to_tsvector('english', description) @@ plainto_tsquery('english', %s)
              AND (country_scope = %s OR country_scope = 'WCO')
            ORDER BY ts_rank(to_tsvector('english', description),
                             plainto_tsquery('english', %s)) DESC
            LIMIT 20
            """,
            (category, country_scope, category),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    return [
        {
            "id": row[0],
            "code": row[1],
            "code_type": row[2],
            "description": row[3],
            "country_scope": row[4],
        }
        for row in rows
    ]


def _insert_mapping(
    category: str,
    classification: dict,
    country_scope: str,
    db_conn,
) -> None:
    """INSERT a mapping into kb_product_hs_mappings."""
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO kb_product_hs_mappings
                (product_category, hs_code, code_type, confidence,
                 reasoning, national_variant, country_scope,
                 source, verified)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                category,
                classification.get("code", ""),
                classification.get("code_type"),
                classification.get("confidence", 0.0),
                classification.get("reasoning"),
                classification.get("national_variant"),
                country_scope,
                "opus_initial",
                False,
            ),
        )
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()


# ------------------------------------------------------------------
# main
# ------------------------------------------------------------------

def main():
    """Entry point: build initial HS mappings.

    Fill in product_categories before running.
    """
    logging.basicConfig(level=logging.INFO)

    db_conn = psycopg2.connect(os.environ["DATABASE_URL"])
    llm = LLMClient()

    # Placeholder — fill in with actual product categories before running
    product_categories = [
        "centrifugal pumps",
        "hydraulic presses",
        "CNC milling machines",
        "safety relief valves",
        "hot-water boilers",
        "electric motors",
        "industrial robots",
        "power transformers",
    ]

    try:
        result = build_initial_hs_mappings(
            product_categories=product_categories,
            country_scope="WCO",
            db_conn=db_conn,
            llm=llm,
        )
        logger.info("Final result: %s", result)
    finally:
        db_conn.close()


if __name__ == "__main__":
    main()
