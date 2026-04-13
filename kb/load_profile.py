"""Upsert functions for KB country profiles and related tables.

Only applies changes routed as 'auto_accept' by the confidence router.
Spot-check and hold items are sent to kb_verification_queue instead.
All DB writes use parameterised queries — no f-string SQL.
"""

import json
import logging

from kb.score_confidence import route_field, queue_item

logger = logging.getLogger(__name__)

# EU member states (ISO2)
EU_MEMBER_STATES = frozenset({
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
})


def upsert_country(profile_data: dict, db_conn) -> int:
    """Upsert into kb_country_profiles. Returns country_id.

    Links to EU block if iso2 is an EU member state.
    """
    iso2 = profile_data["iso2"]
    cur = db_conn.cursor()
    try:
        # Resolve EU block_id if applicable
        block_id = None
        if iso2 in EU_MEMBER_STATES:
            cur.execute(
                "SELECT id FROM kb_economic_blocks WHERE code = %s",
                ("EU",),
            )
            row = cur.fetchone()
            if row:
                block_id = row[0]

        cur.execute(
            """
            INSERT INTO kb_country_profiles (iso2, iso3, country_name, region, block_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (iso2) DO UPDATE SET
                country_name = EXCLUDED.country_name,
                region = EXCLUDED.region,
                block_id = COALESCE(EXCLUDED.block_id, kb_country_profiles.block_id),
                updated_at = NOW()
            RETURNING id
            """,
            (
                iso2,
                profile_data.get("iso3", ""),
                profile_data.get("country_name", ""),
                profile_data.get("region"),
                block_id,
            ),
        )
        country_id = cur.fetchone()[0]
        db_conn.commit()
        return country_id
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()


def upsert_memberships(
    country_id: int,
    memberships: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_memberships. Only auto_accept items are written.

    Returns {"accepted": int, "queued": int}.
    """
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for m in memberships:
            confidence = m.get("confidence", 0.5)
            route = route_field("org_code", m.get("org_code"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_memberships
                        (country_id, org_code, is_member, membership_type,
                         accession_date, scope_details, source_url,
                         confidence, last_verified_at, extraction_method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (country_id, org_code) DO UPDATE SET
                        is_member = EXCLUDED.is_member,
                        membership_type = EXCLUDED.membership_type,
                        accession_date = EXCLUDED.accession_date,
                        scope_details = EXCLUDED.scope_details,
                        source_url = EXCLUDED.source_url,
                        confidence = EXCLUDED.confidence,
                        last_verified_at = EXCLUDED.last_verified_at,
                        extraction_method = EXCLUDED.extraction_method
                    """,
                    (
                        country_id,
                        m.get("org_code"),
                        m.get("is_member"),
                        m.get("membership_type"),
                        m.get("accession_date"),
                        m.get("scope_details"),
                        m.get("source_url", ""),
                        confidence,
                        m.get("last_verified_at"),
                        m.get("extraction_method", "direct_parse"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_memberships",
                    record_id=None,
                    field_name="org_code",
                    current_value=None,
                    proposed_value=json.dumps(m),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=m.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}


def upsert_standards(
    country_id: int,
    standards: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_standards_acceptance. Only auto_accept items are written."""
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for s in standards:
            confidence = s.get("confidence", 0.5)
            route = route_field("standard_code", s.get("standard_code"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_standards_acceptance
                        (country_id, standard_code, standard_name, standard_type,
                         accepted, national_equivalent, harmonization_level,
                         comments, source_url, confidence, last_verified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (country_id, standard_code) DO UPDATE SET
                        standard_name = EXCLUDED.standard_name,
                        standard_type = EXCLUDED.standard_type,
                        accepted = EXCLUDED.accepted,
                        national_equivalent = EXCLUDED.national_equivalent,
                        harmonization_level = EXCLUDED.harmonization_level,
                        comments = EXCLUDED.comments,
                        source_url = EXCLUDED.source_url,
                        confidence = EXCLUDED.confidence,
                        last_verified_at = EXCLUDED.last_verified_at
                    """,
                    (
                        country_id,
                        s.get("standard_code"),
                        s.get("standard_name"),
                        s.get("standard_type"),
                        s.get("accepted"),
                        s.get("national_equivalent"),
                        s.get("harmonization_level"),
                        s.get("comments"),
                        s.get("source_url"),
                        confidence,
                        s.get("last_verified_at"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_standards_acceptance",
                    record_id=None,
                    field_name="standard_code",
                    current_value=None,
                    proposed_value=json.dumps(s),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=s.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}


def upsert_laws(
    country_id: int,
    laws: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_laws. Only auto_accept items are written."""
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for law in laws:
            confidence = law.get("confidence", 0.5)
            route = route_field("title", law.get("title"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_laws
                        (country_id, title, law_type, scope, url,
                         standards_mandatory, local_adaptation_notes,
                         source_url, confidence, last_verified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        country_id,
                        law.get("title"),
                        law.get("law_type"),
                        law.get("scope"),
                        law.get("url"),
                        law.get("standards_mandatory"),
                        law.get("local_adaptation_notes"),
                        law.get("source_url"),
                        confidence,
                        law.get("last_verified_at"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_laws",
                    record_id=None,
                    field_name="title",
                    current_value=None,
                    proposed_value=json.dumps(law),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=law.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}


def upsert_mras(
    country_id: int,
    mras: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_mras. Only auto_accept items are written."""
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for mra in mras:
            confidence = mra.get("confidence", 0.5)
            route = route_field("name", mra.get("name"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_mras
                        (country_id, mra_type, partner, name, scope,
                         membership_role, sectors_covered, signed_date,
                         effective_date, source_url, confidence, last_verified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        country_id,
                        mra.get("mra_type"),
                        mra.get("partner"),
                        mra.get("name"),
                        mra.get("scope"),
                        mra.get("membership_role"),
                        mra.get("sectors_covered"),
                        mra.get("signed_date"),
                        mra.get("effective_date"),
                        mra.get("source_url"),
                        confidence,
                        mra.get("last_verified_at"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_mras",
                    record_id=None,
                    field_name="name",
                    current_value=None,
                    proposed_value=json.dumps(mra),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=mra.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}


def upsert_deviations(
    country_id: int,
    deviations: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_national_deviations. Only auto_accept items are written."""
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for dev in deviations:
            confidence = dev.get("confidence", 0.5)
            route = route_field("reference_standard", dev.get("reference_standard"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_national_deviations
                        (country_id, reference_standard, deviation_type,
                         description, documentation_required,
                         source_url, confidence, last_verified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        country_id,
                        dev.get("reference_standard"),
                        dev.get("deviation_type"),
                        dev.get("description"),
                        json.dumps(dev.get("documentation_required", [])),
                        dev.get("source_url"),
                        confidence,
                        dev.get("last_verified_at"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_national_deviations",
                    record_id=None,
                    field_name="reference_standard",
                    current_value=None,
                    proposed_value=json.dumps(dev),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=dev.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}


def upsert_testing_protocols(
    country_id: int,
    protocols: list[dict],
    db_conn,
) -> dict:
    """Upsert into kb_testing_protocols. Only auto_accept items are written."""
    accepted = 0
    queued = 0
    cur = db_conn.cursor()
    try:
        for p in protocols:
            confidence = p.get("confidence", 0.5)
            route = route_field("protocol_name", p.get("protocol_name"), confidence)

            if route == "auto_accept":
                cur.execute(
                    """
                    INSERT INTO kb_testing_protocols
                        (country_id, protocol_name, accepted,
                         accepted_conditionally, conditions, notes,
                         source_url, confidence, last_verified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (country_id, protocol_name) DO UPDATE SET
                        accepted = EXCLUDED.accepted,
                        accepted_conditionally = EXCLUDED.accepted_conditionally,
                        conditions = EXCLUDED.conditions,
                        notes = EXCLUDED.notes,
                        source_url = EXCLUDED.source_url,
                        confidence = EXCLUDED.confidence,
                        last_verified_at = EXCLUDED.last_verified_at
                    """,
                    (
                        country_id,
                        p.get("protocol_name"),
                        p.get("accepted"),
                        p.get("accepted_conditionally", False),
                        json.dumps(p.get("conditions", [])),
                        p.get("notes"),
                        p.get("source_url"),
                        confidence,
                        p.get("last_verified_at"),
                    ),
                )
                accepted += 1
            else:
                issue_type = "spot_check" if route == "spot_check" else "low_confidence"
                queue_item(
                    country_id=country_id,
                    table_name="kb_testing_protocols",
                    record_id=None,
                    field_name="protocol_name",
                    current_value=None,
                    proposed_value=json.dumps(p),
                    confidence=confidence,
                    issue_type=issue_type,
                    source_url=p.get("source_url"),
                    db_conn=db_conn,
                )
                queued += 1

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cur.close()

    return {"accepted": accepted, "queued": queued}
