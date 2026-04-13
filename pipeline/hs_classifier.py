"""HS code classification for the regulation pipeline.

Uses a two-tier strategy:
  1. Lookup: check kb_product_hs_mappings for a verified mapping.
  2. Classify: GIN full-text search for candidates, then LLM classification.
     New mappings are stored for future lookups and queued for review.

All DB writes use parameterised queries.  No f-string SQL.
classify_hs_code() in LLMClient already scales confidence by 0.85.
"""

import json
import logging

from pipeline.llm_client import LLMClient
from kb.load_profile import EU_MEMBER_STATES

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# get_country_scope
# ------------------------------------------------------------------

def get_country_scope(country_code: str, db_conn) -> str:
    """Determine the HS code scope for a country.

    EU members -> 'EU_CN_8', US -> 'US_HTS_10', else 'WCO_6'.
    Falls back to 'WCO_6' if the country is not in the KB.
    """
    if country_code == "US":
        return "US_HTS_10"

    if country_code in EU_MEMBER_STATES or country_code == "EU":
        return "EU_CN_8"

    # Check DB for block membership (catches EU candidates / new members)
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            SELECT eb.code
            FROM kb_country_profiles cp
            JOIN kb_economic_blocks eb ON cp.block_id = eb.id
            WHERE cp.iso2 = %s
            """,
            (country_code,),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if row and row[0] == "EU":
        return "EU_CN_8"

    return "WCO_6"


# ------------------------------------------------------------------
# lookup_mapping
# ------------------------------------------------------------------

def lookup_mapping(
    product_category: str,
    country_scope: str,
    db_conn,
) -> dict | None:
    """Look up a verified HS mapping from kb_product_hs_mappings.

    Returns the first verified match as a dict, or None.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            SELECT hs_code, code_type, confidence, reasoning, national_variant
            FROM kb_product_hs_mappings
            WHERE product_category = %s
              AND country_scope = %s
              AND verified = TRUE
            ORDER BY confidence DESC
            LIMIT 1
            """,
            (product_category, country_scope),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if row is None:
        return None

    if isinstance(row, dict):
        return {
            "code": row["hs_code"],
            "code_type": row["code_type"],
            "confidence": row["confidence"],
            "reasoning": row["reasoning"],
            "national_variant": row["national_variant"],
        }
    return {
        "code": row[0],
        "code_type": row[1],
        "confidence": row[2],
        "reasoning": row[3],
        "national_variant": row[4],
    }


# ------------------------------------------------------------------
# get_candidates_by_keyword
# ------------------------------------------------------------------

def get_candidates_by_keyword(
    product_category: str,
    country_scope: str,
    db_conn,
    limit: int = 20,
) -> list[dict]:
    """GIN full-text search against kb_hs_codes.description.

    Filters by country_scope (with WCO fallback).
    Returns up to *limit* rows ranked by relevance.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            SELECT code, code_type, description
            FROM kb_hs_codes
            WHERE to_tsvector('english', description)
                  @@ plainto_tsquery('english', %s)
              AND (country_scope = %s OR country_scope = 'WCO')
            ORDER BY ts_rank(
                to_tsvector('english', description),
                plainto_tsquery('english', %s)
            ) DESC
            LIMIT %s
            """,
            (product_category, country_scope, product_category, limit),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    return [
        {
            "code": r["code"] if isinstance(r, dict) else r[0],
            "code_type": r["code_type"] if isinstance(r, dict) else r[1],
            "description": r["description"] if isinstance(r, dict) else r[2],
        }
        for r in rows
    ]


# ------------------------------------------------------------------
# store_new_mapping
# ------------------------------------------------------------------

def store_new_mapping(
    product_category: str,
    classification: dict,
    country_scope: str,
    db_conn,
) -> None:
    """INSERT a new mapping into kb_product_hs_mappings.

    source='opus_ingestion', verified=False.
    ON CONFLICT DO NOTHING — safe for parallel runs.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO kb_product_hs_mappings
                (product_category, hs_code, code_type, confidence,
                 reasoning, national_variant, country_scope,
                 source, verified)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                product_category,
                classification.get("code", ""),
                classification.get("code_type"),
                classification.get("confidence", 0.0),
                classification.get("reasoning"),
                classification.get("national_variant"),
                country_scope,
                "opus_ingestion",
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
# queue_for_review
# ------------------------------------------------------------------

def queue_for_review(
    product_category: str,
    classification: dict,
    country_scope: str,
    db_conn,
) -> None:
    """Queue a new HS mapping for human review in validation_queue."""
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO validation_queue
                (record_type, issue_type, issue_detail, status)
            VALUES (%s, %s, %s, %s)
            """,
            (
                "hs_mapping",
                "new_mapping",
                json.dumps({
                    "product_category": product_category,
                    "code": classification.get("code"),
                    "code_type": classification.get("code_type"),
                    "confidence": classification.get("confidence"),
                    "reasoning": classification.get("reasoning"),
                    "country_scope": country_scope,
                }),
                "pending",
            ),
        )
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()


# ------------------------------------------------------------------
# classify_regulation
# ------------------------------------------------------------------

def classify_regulation(
    regulation: dict,
    db_conn,
    llm: LLMClient,
) -> list[dict]:
    """Classify all product categories in a regulation record.

    For each product_category:
      1. Try lookup_mapping() — if found, use it (mapping_method='lookup').
      2. Otherwise, get_candidates_by_keyword() -> llm.classify_hs_code()
         -> store_new_mapping() + queue_for_review().

    Returns a list of classification results, one per category.
    Each result: {product_category, code, code_type, confidence,
                  reasoning, national_variant, mapping_method}
    """
    categories = regulation.get("product_categories", [])
    country_code = regulation.get("country", "")
    country_scope = get_country_scope(country_code, db_conn)

    results: list[dict] = []

    for category in categories:
        category = category.strip()
        if not category:
            continue

        try:
            result = _classify_single(category, country_scope, db_conn, llm)
            results.append(result)
        except Exception as exc:
            logger.error(
                "HS classification failed for '%s': %s",
                category,
                exc,
            )

    return results


def _classify_single(
    category: str,
    country_scope: str,
    db_conn,
    llm: LLMClient,
) -> dict:
    """Classify a single product category."""
    # 1. Try verified lookup
    existing = lookup_mapping(category, country_scope, db_conn)
    if existing:
        logger.debug("Lookup hit for '%s': %s", category, existing["code"])
        return {
            "product_category": category,
            "mapping_method": "lookup",
            **existing,
        }

    # 2. Full-text search + LLM
    candidates = get_candidates_by_keyword(category, country_scope, db_conn)
    if not candidates:
        logger.warning(
            "No HS candidates found for '%s' (scope=%s)",
            category,
            country_scope,
        )
        return {
            "product_category": category,
            "code": None,
            "code_type": None,
            "confidence": 0.0,
            "reasoning": "No candidates found in HS code library",
            "national_variant": None,
            "mapping_method": "no_candidates",
        }

    classification = llm.classify_hs_code(category, candidates)

    # Store for future lookups
    store_new_mapping(category, classification, country_scope, db_conn)

    # Queue for human review
    queue_for_review(category, classification, country_scope, db_conn)

    logger.info(
        "Classified '%s' -> %s (confidence=%.3f, method=llm_rag)",
        category,
        classification.get("code"),
        classification.get("confidence", 0),
    )

    return {
        "product_category": category,
        "mapping_method": "llm_rag",
        **classification,
    }
