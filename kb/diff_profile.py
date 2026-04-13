"""Diff utilities for detecting meaningful changes in KB country profiles.

Used before writing updates to avoid unnecessary DB churn and to route
only genuinely changed fields through the confidence/review pipeline.
"""

import difflib
import json
import logging

logger = logging.getLogger(__name__)

TEXT_SIMILARITY_THRESHOLD = 0.85


def diff_scalar(existing_value, extracted_value) -> bool:
    """Return True if values differ meaningfully (not just whitespace).

    Handles None, strings, numbers, and booleans.
    """
    if existing_value is None and extracted_value is None:
        return False
    if existing_value is None or extracted_value is None:
        return True

    # Normalise strings: strip and collapse whitespace
    if isinstance(existing_value, str) and isinstance(extracted_value, str):
        e = " ".join(existing_value.split())
        x = " ".join(extracted_value.split())
        return e != x

    return existing_value != extracted_value


def diff_text(existing: str | None, extracted: str | None) -> bool:
    """Return True if texts differ by more than 15%.

    Uses difflib.SequenceMatcher with a 0.85 similarity threshold.
    If either value is None, falls back to scalar comparison.
    """
    if existing is None or extracted is None:
        return diff_scalar(existing, extracted)

    ratio = difflib.SequenceMatcher(None, existing, extracted).ratio()
    return ratio < TEXT_SIMILARITY_THRESHOLD


def diff_list(
    existing: list | None,
    extracted: list | None,
    key_field: str,
) -> list[dict]:
    """Return items in *extracted* that are new or changed vs *existing*.

    Uses *key_field* (e.g. 'org_code', 'standard_code') to match records
    between the two lists.  Returns only the changed/new items from
    *extracted*.
    """
    if not extracted:
        return []
    if not existing:
        return list(extracted)

    existing_by_key = {}
    for item in existing:
        key = item.get(key_field)
        if key is not None:
            existing_by_key[key] = item

    changed: list[dict] = []
    for item in extracted:
        key = item.get(key_field)
        if key is None:
            changed.append(item)
            continue

        old = existing_by_key.get(key)
        if old is None:
            # New item
            changed.append(item)
            continue

        # Compare field by field (excluding metadata)
        if _record_differs(old, item):
            changed.append(item)

    return changed


def diff_country_profile(
    country_id: int,
    extracted: dict,
    db_conn,
) -> dict:
    """Orchestrate diffs across all profile sections.

    Reads the current profile from the database, compares with *extracted*,
    and returns {changed: [...], unchanged_count: int}.

    Each entry in changed is:
        {table, field, old_value, new_value, change_type}
    """
    cur = db_conn.cursor()
    changed: list[dict] = []
    unchanged_count = 0

    try:
        # --- Scalar / JSONB fields on kb_country_profiles ---
        cur.execute(
            """
            SELECT national_standards_body, accreditation_body,
                   metrology_institute, legal_metrology_body,
                   local_challenges, recent_reforms, general_notes,
                   translation_requirements, translation_notes,
                   ca_system_structure, accreditation_mandatory
            FROM kb_country_profiles WHERE id = %s
            """,
            (country_id,),
        )
        row = cur.fetchone()
        if row is None:
            # No existing profile — everything is new
            for key, val in extracted.items():
                changed.append({
                    "table": "kb_country_profiles",
                    "field": key,
                    "old_value": None,
                    "new_value": val,
                    "change_type": "new",
                })
            return {"changed": changed, "unchanged_count": 0}

        profile_fields = [
            "national_standards_body", "accreditation_body",
            "metrology_institute", "legal_metrology_body",
            "local_challenges", "recent_reforms", "general_notes",
            "translation_requirements", "translation_notes",
            "ca_system_structure", "accreditation_mandatory",
        ]
        for i, field in enumerate(profile_fields):
            if field not in extracted:
                continue
            old_val = row[i]
            new_val = extracted[field]

            # JSONB fields come back as dicts already from psycopg2
            if isinstance(old_val, str) and isinstance(new_val, str):
                differs = diff_text(old_val, new_val)
            else:
                differs = diff_scalar(
                    _normalise_for_compare(old_val),
                    _normalise_for_compare(new_val),
                )

            if differs:
                changed.append({
                    "table": "kb_country_profiles",
                    "field": field,
                    "old_value": old_val,
                    "new_value": new_val,
                    "change_type": "updated",
                })
            else:
                unchanged_count += 1

        # --- List tables ---
        list_tables = {
            "memberships": ("kb_memberships", "org_code"),
            "standards_acceptance": ("kb_standards_acceptance", "standard_code"),
            "testing_protocols": ("kb_testing_protocols", "protocol_name"),
            "national_deviations": ("kb_national_deviations", "reference_standard"),
            "laws": ("kb_laws", "title"),
        }

        for section, (table, key_field) in list_tables.items():
            if section not in extracted:
                continue

            cur.execute(
                f"SELECT * FROM {table} WHERE country_id = %s",
                (country_id,),
            )
            columns = [desc[0] for desc in cur.description]
            existing_rows = [dict(zip(columns, r)) for r in cur.fetchall()]

            new_items = diff_list(existing_rows, extracted[section], key_field)

            for item in new_items:
                key_val = item.get(key_field, "unknown")
                old_match = next(
                    (e for e in existing_rows if e.get(key_field) == key_val),
                    None,
                )
                changed.append({
                    "table": table,
                    "field": key_field,
                    "old_value": old_match,
                    "new_value": item,
                    "change_type": "new" if old_match is None else "updated",
                })

            unchanged_count += len(extracted[section]) - len(new_items)

    finally:
        cur.close()

    return {"changed": changed, "unchanged_count": unchanged_count}


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _record_differs(old: dict, new: dict) -> bool:
    """Compare two dicts field-by-field, ignoring metadata keys."""
    metadata_keys = {
        "id", "country_id", "created_at", "updated_at",
        "last_verified_at", "confidence", "source_url",
        "extraction_method",
    }
    all_keys = set(old.keys()) | set(new.keys())
    compare_keys = all_keys - metadata_keys

    for key in compare_keys:
        old_val = old.get(key)
        new_val = new.get(key)
        if diff_scalar(old_val, new_val):
            return True
    return False


def _normalise_for_compare(value):
    """Convert JSONB (dict/list) to a comparable form."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value
