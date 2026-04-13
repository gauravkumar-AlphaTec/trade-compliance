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
# quarantine
# ------------------------------------------------------------------

def quarantine(record: dict, issues: list[str], db_conn) -> None:
    """Insert each issue into the validation_queue table.

    Parameters
    ----------
    record : dict
        The regulation record that failed validation.
    issues : list[str]
        Human-readable issue descriptions.
    db_conn :
        Database connection.
    """
    cur = db_conn.cursor()
    try:
        for issue_text in issues:
            # Parse issue_type from the text if it follows "type: detail" pattern
            if ": " in issue_text:
                issue_type, issue_detail = issue_text.split(": ", 1)
            else:
                issue_type = "validation_failure"
                issue_detail = issue_text

            cur.execute(
                """
                INSERT INTO validation_queue
                    (record_type, issue_type, issue_detail, status)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "regulation",
                    issue_type,
                    issue_detail,
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
# validate_and_route
# ------------------------------------------------------------------

def validate_and_route(record: dict, db_conn, source: dict | None = None) -> str:
    """Validate a regulation record and route it.

    Returns
    -------
    str
        'pass' — record is valid, proceed to load.
        'quarantine' — record has issues, inserted into validation_queue.
        'duplicate' — record is a duplicate (_skip flag set by clean stage).
    """
    # Check for duplicate (set by pipeline/process.py clean stage)
    if record.get("_skip"):
        return "duplicate"

    result = validate_regulation(record, source)

    if result["status"] == "quarantine":
        issues = [
            f"{i['issue_type']}: {i['issue_detail']}"
            for i in result["issues"]
        ]
        quarantine(record, issues, db_conn)
        return "quarantine"

    return "pass"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_celex_year(document_id: str) -> int | None:
    """Extract the year from a CELEX number like '32006L0042'."""
    match = re.search(r"(\d)(\d{4})", document_id)
    if match:
        return int(match.group(2))
    return None
