"""Validation logic for regulation records before they enter the database.

Each check appends issues to a list.  validate_regulation() returns
'pass' if the list is empty, 'quarantine' otherwise, along with the issues.
"""

import logging
import re

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("title", "country", "document_type")

GENERIC_CATEGORIES = frozenset({
    "pressure equipment",
    "assemblies",
    "machinery",
    "equipment",
    "products",
    "goods",
    "articles",
    "devices",
})


def validate_regulation(record: dict, source: dict | None = None) -> dict:
    """Run all validation checks on a regulation record.

    Parameters
    ----------
    record : dict
        Regulation fields (title, country, effective_date, product_categories, …).
    source : dict | None
        Source provenance (source_name, document_id, …).  Optional.

    Returns
    -------
    dict
        {"status": "pass" | "quarantine", "issues": [...]}
        Each issue is {"issue_type": str, "issue_detail": str}.
    """
    issues: list[dict] = []

    # 1. Required fields
    _check_required_fields(record, issues)

    # 2. Low confidence
    _check_low_confidence(record, issues)

    # 3. Date sanity
    _check_date_sanity(record, source, issues)

    # 4. Product categories depth
    _check_categories_depth(record, issues)

    # 5. Duplicate (always last)
    _check_duplicate(record, issues)

    status = "pass" if not issues else "quarantine"
    return {"status": status, "issues": issues}


# ------------------------------------------------------------------
# Individual checks
# ------------------------------------------------------------------

def _check_required_fields(record: dict, issues: list[dict]) -> None:
    for field in REQUIRED_FIELDS:
        if not record.get(field):
            issues.append({
                "issue_type": "missing_field",
                "issue_detail": f"Required field '{field}' is missing or empty",
            })


def _check_low_confidence(record: dict, issues: list[dict]) -> None:
    confidence = record.get("confidence")
    if confidence is not None and confidence < 0.5:
        issues.append({
            "issue_type": "low_confidence",
            "issue_detail": f"Extraction confidence {confidence} is below 0.5 threshold",
        })


def _check_date_sanity(
    record: dict, source: dict | None, issues: list[dict]
) -> None:
    """Flag if effective_date looks like a signing/publication date.

    For EUR-Lex sources, compares effective_date year against the CELEX
    number year (first 4 digits after the sector letter, e.g. 32006L0042 → 2006).
    """
    effective_date = record.get("effective_date")
    if not effective_date or not source:
        return

    source_name = source.get("source_name", "")
    if source_name != "EUR-Lex":
        return

    document_id = source.get("document_id", "")
    celex_year = _extract_celex_year(document_id)
    if celex_year is None:
        return

    # effective_date may be a string "YYYY-MM-DD" or a date object
    eff_str = str(effective_date)
    if eff_str.startswith(str(celex_year)):
        issues.append({
            "issue_type": "suspect_date",
            "issue_detail": (
                "effective_date may be signing date not entry into force date"
            ),
        })


def _check_categories_depth(record: dict, issues: list[dict]) -> None:
    """Flag if product_categories are too generic for HS classification."""
    categories = record.get("product_categories")
    if not categories:
        return

    if len(categories) >= 3:
        return

    all_generic = all(
        cat.strip().lower() in GENERIC_CATEGORIES for cat in categories
    )
    if all_generic:
        issues.append({
            "issue_type": "shallow_categories",
            "issue_detail": (
                "categories too generic for HS classification — "
                "re-extract with more specific prompt"
            ),
        })


def _check_duplicate(record: dict, issues: list[dict]) -> None:
    """Placeholder for duplicate detection — requires DB access."""
    # Duplicate checking is deferred to the DB upsert layer
    pass


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_celex_year(document_id: str) -> int | None:
    """Extract the year from a CELEX number like '32006L0042'."""
    match = re.search(r"(\d)(\d{4})", document_id)
    if match:
        return int(match.group(2))
    return None
