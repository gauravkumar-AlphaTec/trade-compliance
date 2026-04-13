"""Confidence scoring and verification-queue routing for KB records.

Implements the trust-tier formula and threshold-based routing from CLAUDE.md:
  >= 0.90  auto-accept
  0.70-0.89  spot-check
  < 0.70  hold for manual review

Source trust tier adjustments:
  Tier 1 (+0.15): WTO, ISO, BIPM, ILAC, IAF
  Tier 2 (+0.05): National standards body
  Tier 3 (+0.00): Government ministry / regulatory body
  Tier 4 (-0.10): Secondary sources, third-party databases
"""

import logging

logger = logging.getLogger(__name__)

# Tier adjustments
TIER_ADJUSTMENTS = {
    1: 0.15,
    2: 0.05,
    3: 0.00,
    4: -0.10,
}

# Free-form text fields that always route to spot_check
FREE_TEXT_FIELDS = frozenset({
    "local_challenges",
    "recent_reforms",
    "general_notes",
    "translation_notes",
    "local_adaptation_notes",
    "scope_details",
    "comments",
    "notes",
    "description",
    "reviewer_note",
})


def calculate_confidence(
    raw_confidence: float,
    source_tier: int,
    cross_validated: bool = False,
    conflict_detected: bool = False,
) -> float:
    """Apply trust-tier adjustment and validation modifiers to raw confidence.

    Parameters
    ----------
    raw_confidence : float
        Base confidence from extraction (0.0-1.0).
    source_tier : int
        Source trust tier (1-4).
    cross_validated : bool
        Whether the value was confirmed by a second source.
    conflict_detected : bool
        Whether conflicting information was found.

    Returns
    -------
    float
        Final confidence clamped to [0.0, 1.0].
    """
    tier_adj = TIER_ADJUSTMENTS.get(source_tier, 0.0)
    confidence = raw_confidence + tier_adj

    if cross_validated:
        confidence += 0.10
    if conflict_detected:
        confidence -= 0.20

    return max(0.0, min(1.0, confidence))


def route_field(field_name: str, value, confidence: float) -> str:
    """Decide how to handle a field based on its confidence score.

    Free-form text fields always return 'spot_check' regardless of confidence
    (per CLAUDE.md: free-form text defaults to confidence=0.65).

    Returns
    -------
    str
        'auto_accept', 'spot_check', or 'hold'.
    """
    if field_name in FREE_TEXT_FIELDS:
        return "spot_check"

    if confidence >= 0.90:
        return "auto_accept"
    if confidence >= 0.70:
        return "spot_check"
    return "hold"


def queue_item(
    country_id: int,
    table_name: str,
    record_id: int | None,
    field_name: str | None,
    current_value: str | None,
    proposed_value: str | None,
    confidence: float,
    issue_type: str,
    source_url: str | None,
    db_conn,
) -> int:
    """Insert a record into kb_verification_queue.

    Returns the new queue item id.
    """
    cur = db_conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO kb_verification_queue
                (country_id, table_name, record_id, field_name,
                 current_value, proposed_value, confidence,
                 issue_type, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                country_id,
                table_name,
                record_id,
                field_name,
                current_value,
                proposed_value,
                confidence,
                issue_type,
                source_url,
            ),
        )
        row = cur.fetchone()
        db_conn.commit()
        return row[0]
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()
