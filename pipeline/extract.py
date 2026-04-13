"""Extraction helpers for the regulation pipeline.

Handles both structured sources (Federal Register JSON, EUR-Lex metadata)
and unstructured sources (full-text documents requiring LLM extraction).
"""

import json
import logging

from pipeline.llm_client import LLMClient
from pipeline.validate import GENERIC_CATEGORIES

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Extraction schema for LLM-based extraction
# ------------------------------------------------------------------

EXTRACTION_SCHEMA = {
    "title": "string",
    "document_type": "string",
    "authority": "string",
    "country": "string",
    "effective_date": "string or null (YYYY-MM-DD)",
    "expiry_date": "string or null (YYYY-MM-DD)",
    "summary": "string",
    "product_categories": ["string"],
    "standards_referenced": ["string"],
    "confidence": "float 0.0-1.0",
}

# ------------------------------------------------------------------
# Result keys — every extract path returns these
# ------------------------------------------------------------------

RESULT_KEYS = (
    "title", "document_type", "authority", "country",
    "effective_date", "expiry_date", "summary",
    "product_categories", "standards_referenced",
    "confidence", "extraction_method",
    "source_name", "document_id",
)

# ------------------------------------------------------------------
# Re-extraction prompts (for shallow categories fix)
# ------------------------------------------------------------------

RE_EXTRACT_SYSTEM = (
    "You are extracting specific product types from a legal document. "
    "Be maximally specific. Never return generic terms like 'equipment' or "
    "'machinery' alone. Always drill down to the actual product: not "
    "'pressure equipment' but 'fired steam boilers', 'hot-water central "
    "heating boilers', 'safety relief valves'."
)

RE_EXTRACT_USER = (
    "List every specific product type mentioned in this document. "
    "Include products mentioned in annexes, schedules, or defined terms "
    "sections — not just the main body. Return ONLY a JSON array of strings. "
    "Each string must be a specific product name, not a category umbrella."
    "\n\nDocument:\n{text}"
)

# ------------------------------------------------------------------
# Structured source types that bypass LLM
# ------------------------------------------------------------------

STRUCTURED_SOURCE_TYPES = frozenset({
    "federal_register",
    "eurlex_metadata",
    "structured",
})


# ------------------------------------------------------------------
# get_country_context
# ------------------------------------------------------------------

def get_country_context(country_code: str, db_conn) -> str:
    """Read KB data for a country and return a brief regulatory context paragraph.

    Used as system_context in LLM extraction calls to improve accuracy.
    Returns empty string if the country is not in the KB.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            SELECT cp.country_name, cp.iso2,
                   cp.national_standards_body,
                   eb.code AS block_code, eb.name AS block_name
            FROM kb_country_profiles cp
            LEFT JOIN kb_economic_blocks eb ON cp.block_id = eb.id
            WHERE cp.iso2 = %s
            """,
            (country_code,),
        )
        row = cur.fetchone()
        if not row:
            return ""

        country_name, iso2, nsb_json, block_code, block_name = row

        # Fetch memberships
        cur.execute(
            """
            SELECT org_code, membership_type
            FROM kb_memberships
            WHERE country_id = (
                SELECT id FROM kb_country_profiles WHERE iso2 = %s
            ) AND is_member = TRUE
            """,
            (country_code,),
        )
        memberships = cur.fetchall()
    finally:
        cur.close()

    # Build context paragraph
    parts = [f"{country_name}"]

    if block_code:
        parts.append(f"is an {block_name} member state using {block_code} harmonized standards (CE marking)")

    # NSB
    if nsb_json:
        nsb = nsb_json if isinstance(nsb_json, dict) else {}
        nsb_value = nsb.get("value", {}) if isinstance(nsb, dict) else {}
        nsb_name = nsb_value.get("acronym") or nsb_value.get("name")
        if nsb_name:
            parts.append(f"National standards body: {nsb_name}")

    # Memberships
    org_parts = []
    for org_code, mem_type in memberships:
        if org_code in ("ILAC", "IAF"):
            org_parts.append(f"{org_code} MRA signatory")
        elif org_code == "WTO":
            org_parts.append("WTO member")

    if org_parts:
        parts.append(". ".join(org_parts))

    return ". ".join(parts) + "."


# ------------------------------------------------------------------
# extract_structured_source
# ------------------------------------------------------------------

def extract_structured_source(raw: dict, source_type: str) -> dict:
    """Extract from structured JSON/XML sources without LLM.

    Handles Federal Register JSON and EUR-Lex metadata API responses.
    Sets extraction_method='structured'.
    """
    result = _empty_result()
    result["extraction_method"] = "structured"
    result["source_name"] = raw.get("source_name", "")
    result["document_id"] = raw.get("document_id", "")

    if source_type == "federal_register":
        result["title"] = raw.get("title", "")
        result["document_type"] = raw.get("document_type", "regulation")
        result["authority"] = raw.get("authority", "")
        result["country"] = raw.get("country", "US")
        result["effective_date"] = raw.get("publication_date")
        result["summary"] = raw.get("abstract", "")
        result["confidence"] = 0.95

    elif source_type == "eurlex_metadata":
        result["title"] = raw.get("title", "")
        result["document_type"] = raw.get("document_type", "")
        result["authority"] = raw.get("authority", "")
        result["country"] = raw.get("country", "EU")
        result["effective_date"] = raw.get("effective_date")
        result["product_categories"] = raw.get("eurovoc_descriptors", [])
        result["confidence"] = 0.95

    else:
        # Generic structured: copy matching keys
        for key in RESULT_KEYS:
            if key in raw and key not in ("extraction_method",):
                result[key] = raw[key]
        result["confidence"] = raw.get("confidence", 0.90)

    return result


# ------------------------------------------------------------------
# extract_unstructured_source
# ------------------------------------------------------------------

def extract_unstructured_source(
    text: str,
    source_name: str,
    country_code: str,
    db_conn,
    llm: LLMClient,
) -> dict:
    """Extract regulation fields from unstructured text using LLM.

    Fetches country context from the KB and passes it as the system prompt
    to improve extraction accuracy.
    Sets extraction_method='llm'.
    """
    result = _empty_result()
    result["extraction_method"] = "llm"
    result["source_name"] = source_name

    country_context = get_country_context(country_code, db_conn)

    system_ctx = (
        "You are extracting regulation metadata from a legal document. "
        "Return ONLY valid JSON matching the schema provided."
    )
    if country_context:
        system_ctx += f"\n\nCountry context: {country_context}"

    try:
        extracted = llm.extract_structured(text, EXTRACTION_SCHEMA, system_context=system_ctx)
    except Exception:
        logger.warning("LLM extraction failed for %s source, returning empty", source_name)
        return result

    # Map extracted fields
    for key in RESULT_KEYS:
        if key in extracted and key not in ("extraction_method", "source_name", "document_id"):
            result[key] = extracted[key]

    # Ensure lists
    if not isinstance(result["product_categories"], list):
        result["product_categories"] = []
    if not isinstance(result["standards_referenced"], list):
        result["standards_referenced"] = []

    return result


# ------------------------------------------------------------------
# extract (router)
# ------------------------------------------------------------------

def extract(raw_document: dict, db_conn, llm: LLMClient) -> dict:
    """Route extraction based on source_type field.

    Structured sources (federal_register, eurlex_metadata) use direct
    field mapping. Unstructured sources go through LLM extraction.

    After LLM extraction, checks for shallow product_categories and
    attempts re-extraction with a sharper prompt before returning.

    Always returns a dict with the same keys regardless of path.
    """
    source_type = raw_document.get("source_type", "")

    if source_type in STRUCTURED_SOURCE_TYPES:
        return extract_structured_source(raw_document, source_type)

    # Unstructured path
    text = raw_document.get("full_text", "")
    source_name = raw_document.get("source_name", "")
    country_code = raw_document.get("country", "")
    document_id = raw_document.get("document_id", "")

    result = extract_unstructured_source(text, source_name, country_code, db_conn, llm)
    result["document_id"] = document_id

    # Check for shallow categories and re-extract if needed
    categories = result.get("product_categories", [])
    if categories and _all_generic(categories):
        logger.info("shallow categories detected, re-extracting")
        new_cats = re_extract_categories(text, result, llm)
        result["product_categories"] = new_cats

    return result


# ------------------------------------------------------------------
# re_extract_categories
# ------------------------------------------------------------------

def re_extract_categories(
    text: str,
    existing_record: dict,
    llm: LLMClient,
) -> list[str]:
    """Re-extract product categories with a targeted, more specific prompt.

    Called when validation flags 'shallow_categories'.  If the new extraction
    still contains only generic terms, returns the existing categories
    unchanged and logs a warning.
    """
    schema = {"categories": ["string"]}
    user_prompt = RE_EXTRACT_USER.format(text=text)

    try:
        raw = llm.extract_structured(user_prompt, schema, system_context=RE_EXTRACT_SYSTEM)
    except Exception:
        logger.warning("re_extract_categories: LLM call failed, keeping existing categories")
        return existing_record.get("product_categories", [])

    # extract_structured returns a dict; we expect {"categories": [...]}
    # but the model may return a bare list
    if isinstance(raw, list):
        categories = raw
    else:
        categories = raw.get("categories", [])

    if not categories:
        logger.warning("re_extract_categories: LLM returned empty list, keeping existing")
        return existing_record.get("product_categories", [])

    # Check if still all generic
    if _all_generic(categories):
        logger.warning(
            "re_extract_categories: re-extraction still generic %s, keeping existing",
            categories,
        )
        return existing_record.get("product_categories", [])

    return categories


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _empty_result() -> dict:
    """Return a result dict with all keys set to defaults."""
    return {
        "title": "",
        "document_type": "",
        "authority": "",
        "country": "",
        "effective_date": None,
        "expiry_date": None,
        "summary": "",
        "product_categories": [],
        "standards_referenced": [],
        "confidence": 0.0,
        "extraction_method": "",
        "source_name": "",
        "document_id": "",
    }


def _all_generic(categories: list) -> bool:
    """Check if all categories in the list are generic terms."""
    if not categories:
        return False
    return all(
        cat.strip().lower() in GENERIC_CATEGORIES
        for cat in categories
        if isinstance(cat, str)
    )
