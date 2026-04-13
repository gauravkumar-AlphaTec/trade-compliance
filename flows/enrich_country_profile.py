"""Prefect flow: enrich a single country profile in the KB.

Fetches tier-1 membership data, LLM-extracts QIB/legal/standards/insights,
scores and diffs against existing data, then applies auto-accept items
and queues the rest for review.
"""

import json
import logging
import os

import psycopg2
from prefect import flow, task

from pipeline.llm_client import LLMClient
from kb.sources.wto import fetch_wto_members
from kb.sources.iso_members import fetch_iso_members
from kb.sources.bipm import fetch_bipm_members
from kb.sources.ilac_iaf import fetch_ilac_signatories, fetch_iaf_members
from kb.extract_profile import (
    extract_qib,
    extract_legal_framework,
    extract_standards_deviations,
    extract_testing_protocols,
    extract_insights,
)
from kb.score_confidence import calculate_confidence, route_field, queue_item
from kb.diff_profile import diff_country_profile
from kb.load_profile import (
    upsert_country,
    upsert_memberships,
    upsert_standards,
    upsert_laws,
    upsert_mras,
    upsert_deviations,
    upsert_testing_protocols,
    EU_MEMBER_STATES,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------

@task(name="fetch-tier1")
def fetch_tier1(country_code: str) -> dict:
    """Fetch all tier-1 membership data for a country."""
    memberships = []

    # WTO
    for m in fetch_wto_members():
        if m["iso2"] == country_code:
            memberships.append({
                "org_code": "WTO",
                "is_member": m["is_member"],
                "membership_type": "full",
                "accession_date": m.get("accession_date"),
                "source_url": m["source_url"],
                "confidence": m["confidence"],
                "last_verified_at": m["last_verified_at"],
                "extraction_method": "direct_parse",
            })

    # ISO
    for m in fetch_iso_members():
        if m["iso2"] == country_code:
            memberships.append({
                "org_code": "ISO",
                "is_member": True,
                "membership_type": m.get("member_type", "full"),
                "source_url": m["source_url"],
                "confidence": m["confidence"],
                "last_verified_at": m["last_verified_at"],
                "extraction_method": "direct_parse",
                "scope_details": m.get("nsb_name"),
            })

    # BIPM
    for m in fetch_bipm_members():
        if m["iso2"] == country_code:
            memberships.append({
                "org_code": "BIPM",
                "is_member": m["is_member"],
                "membership_type": m.get("membership_type"),
                "source_url": m["source_url"],
                "confidence": m["confidence"],
                "last_verified_at": m["last_verified_at"],
                "extraction_method": "direct_parse",
            })

    # ILAC
    for m in fetch_ilac_signatories():
        if m["iso2"] == country_code:
            memberships.append({
                "org_code": "ILAC",
                "is_member": m["is_signatory"],
                "membership_type": "signatory",
                "scope_details": m.get("scope"),
                "source_url": m["source_url"],
                "confidence": m["confidence"],
                "last_verified_at": m["last_verified_at"],
                "extraction_method": "direct_parse",
            })

    # IAF
    for m in fetch_iaf_members():
        if m["iso2"] == country_code:
            memberships.append({
                "org_code": "IAF",
                "is_member": m["is_signatory"],
                "membership_type": "signatory",
                "scope_details": m.get("scope"),
                "source_url": m["source_url"],
                "confidence": m["confidence"],
                "last_verified_at": m["last_verified_at"],
                "extraction_method": "direct_parse",
            })

    return {"memberships": memberships}


@task(name="fetch-qib")
def fetch_qib(country_code: str, llm: LLMClient) -> dict:
    """Extract quality infrastructure bodies via LLM."""
    # In production this would fetch from a government portal page
    # For now, use a placeholder prompt with the country code
    return extract_qib(
        country_name=country_code,
        iso2=country_code,
        source_url=f"https://government.{country_code.lower()}/standards",
        page_content=f"Quality infrastructure information for {country_code}",
        llm=llm,
    )


@task(name="fetch-legal")
def fetch_legal(country_code: str, llm: LLMClient) -> dict:
    """Extract legal framework via LLM."""
    records = extract_legal_framework(
        country_name=country_code,
        iso2=country_code,
        page_content=f"Legal framework for {country_code}",
        llm=llm,
    )
    return {"laws": records}


@task(name="fetch-standards")
def fetch_standards(country_code: str, llm: LLMClient) -> dict:
    """Extract standards acceptance and deviations via LLM."""
    is_eu = country_code in EU_MEMBER_STATES
    records = extract_standards_deviations(
        country_name=country_code,
        iso2=country_code,
        is_eu_member=is_eu,
        product_categories=[],
        page_content=f"Standards information for {country_code}",
        llm=llm,
    )
    protocols = extract_testing_protocols(
        country_name=country_code,
        iso2=country_code,
        page_content=f"Testing protocols for {country_code}",
        llm=llm,
    )
    return {"standards": records, "protocols": protocols}


@task(name="fetch-insights")
def fetch_insights(country_code: str, llm: LLMClient) -> dict:
    """Generate insights — always confidence=0.65."""
    return extract_insights(
        country_name=country_code,
        iso2=country_code,
        llm=llm,
    )


@task(name="score-and-diff")
def score_and_diff(country_code: str, extracted: dict, db_conn) -> dict:
    """Score confidence, diff against existing profile, route each field."""
    # Look up country_id
    cur = db_conn.cursor()
    cur.execute(
        "SELECT id FROM kb_country_profiles WHERE iso2 = %s",
        (country_code,),
    )
    row = cur.fetchone()
    cur.close()

    if row is None:
        # New country — everything is new, route by confidence
        routed = {"auto_accept": {}, "spot_check": {}, "hold": {}}
        for section, items in extracted.items():
            if isinstance(items, list):
                for item in items:
                    conf = item.get("confidence", 0.5)
                    route = route_field(section, items, conf)
                    routed.setdefault(route, {}).setdefault(section, []).append(item)
            elif isinstance(items, dict):
                conf = items.get("confidence", 0.5)
                route = route_field(section, items, conf)
                routed.setdefault(route, {})[section] = items
        return routed

    country_id = row[0]
    diff = diff_country_profile(country_id, extracted, db_conn)

    routed = {"auto_accept": {}, "spot_check": {}, "hold": {}}
    for change in diff["changed"]:
        field = change["field"]
        new_val = change["new_value"]
        conf = new_val.get("confidence", 0.5) if isinstance(new_val, dict) else 0.5
        route = route_field(field, new_val, conf)
        bucket = routed[route]
        if change["table"] in bucket:
            if isinstance(bucket[change["table"]], list):
                bucket[change["table"]].append(new_val)
            else:
                bucket[change["table"]] = [bucket[change["table"]], new_val]
        else:
            bucket[change["table"]] = new_val

    return routed


@task(name="apply-and-queue")
def apply_and_queue(country_code: str, routed: dict, db_conn) -> dict:
    """Write auto_accept items to DB. Queue spot_check/hold items.

    Returns summary counts per section.
    """
    summary = {"accepted": 0, "queued": 0, "held": 0}

    # Look up or create country
    cur = db_conn.cursor()
    cur.execute("SELECT id FROM kb_country_profiles WHERE iso2 = %s", (country_code,))
    row = cur.fetchone()
    cur.close()

    if row is None:
        logger.info("Country %s not in DB yet — creating profile", country_code)
        country_id = upsert_country(
            {"iso2": country_code, "iso3": "", "country_name": country_code},
            db_conn,
        )
    else:
        country_id = row[0]

    # Dispatch by table/section
    section_handlers = {
        "memberships": lambda items: upsert_memberships(country_id, items, db_conn),
        "standards": lambda items: upsert_standards(country_id, items, db_conn),
        "laws": lambda items: upsert_laws(country_id, items, db_conn),
        "mras": lambda items: upsert_mras(country_id, items, db_conn),
        "deviations": lambda items: upsert_deviations(country_id, items, db_conn),
        "protocols": lambda items: upsert_testing_protocols(country_id, items, db_conn),
    }

    # Process auto_accept items
    for section, data in routed.get("auto_accept", {}).items():
        handler = section_handlers.get(section)
        if handler:
            items = data if isinstance(data, list) else [data]
            result = handler(items)
            summary["accepted"] += result.get("accepted", 0)
            summary["queued"] += result.get("queued", 0)

    # Queue spot_check and hold items
    for route_type in ("spot_check", "hold"):
        for section, data in routed.get(route_type, {}).items():
            items = data if isinstance(data, list) else [data]
            for item in items:
                issue_type = route_type
                queue_item(
                    country_id=country_id,
                    table_name=f"kb_{section}",
                    record_id=None,
                    field_name=None,
                    current_value=None,
                    proposed_value=json.dumps(item) if isinstance(item, dict) else str(item),
                    confidence=item.get("confidence", 0.5) if isinstance(item, dict) else 0.5,
                    issue_type=issue_type,
                    source_url=item.get("source_url") if isinstance(item, dict) else None,
                    db_conn=db_conn,
                )
                if route_type == "spot_check":
                    summary["queued"] += 1
                else:
                    summary["held"] += 1

    return summary


# ------------------------------------------------------------------
# Flow
# ------------------------------------------------------------------

@flow(name="enrich-country-profile")
def enrich_country_profile(country_code: str) -> dict:
    """Enrich a single country's KB profile.

    Runs all extraction tasks in sequence, scores and diffs, then applies
    auto-accept items and queues the rest.

    Schedule: monthly for EU + US.
    """
    llm = LLMClient()

    if not llm.health_check():
        logger.warning("Ollama health check failed — proceeding anyway")

    # Fetch data from all sources
    tier1 = fetch_tier1(country_code)
    qib = fetch_qib(country_code, llm)
    legal = fetch_legal(country_code, llm)
    standards_data = fetch_standards(country_code, llm)
    insights = fetch_insights(country_code, llm)

    # Merge all extracted data
    extracted = {
        "memberships": tier1.get("memberships", []),
        "laws": legal.get("laws", []),
        "standards": standards_data.get("standards", []),
        "protocols": standards_data.get("protocols", []),
        **qib,
        **insights,
    }

    # Connect to DB
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False

    try:
        # Ensure country exists
        upsert_country(
            {
                "iso2": country_code,
                "iso3": "",
                "country_name": country_code,
            },
            conn,
        )

        # Score, diff, and route
        routed = score_and_diff(country_code, extracted, conn)

        # Apply and queue
        summary = apply_and_queue(country_code, routed, conn)

        logger.info(
            "Enrichment complete for %s: %d accepted, %d queued, %d held",
            country_code,
            summary["accepted"],
            summary["queued"],
            summary["held"],
        )

        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "DE"
    enrich_country_profile(code)
