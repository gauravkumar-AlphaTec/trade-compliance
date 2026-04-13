"""Extraction helpers for the regulation pipeline."""

import json
import logging

from pipeline.llm_client import LLMClient
from pipeline.validate import GENERIC_CATEGORIES

logger = logging.getLogger(__name__)

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
    all_generic = all(
        cat.strip().lower() in GENERIC_CATEGORIES
        for cat in categories
        if isinstance(cat, str)
    )
    if all_generic:
        logger.warning(
            "re_extract_categories: re-extraction still generic %s, keeping existing",
            categories,
        )
        return existing_record.get("product_categories", [])

    return categories
