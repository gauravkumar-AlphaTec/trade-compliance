"""Fetch EU NLF directives and harmonized standards from EC sources."""

import logging
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

NLF_URL = (
    "https://single-market-economy.ec.europa.eu"
    "/single-market/goods/new-legislative-framework_en"
)
CONFIDENCE = 0.95  # Tier 3 source but official EU portal — treated as high
TIMEOUT = 30


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def fetch_eu_directives() -> list[dict]:
    """Fetch active NLF directive list from the EC single-market page.

    Returns list of dicts with keys:
        directive_number, title, ojl_reference, url, scope
    """
    try:
        response = httpx.get(NLF_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("EU directives fetch failed: %s", exc)
        return []

    try:
        return _parse_directives(response.text)
    except Exception as exc:
        logger.warning("EU directives parse failed: %s", exc)
        return []


def fetch_harmonized_standards(directive: str) -> list[dict]:
    """Fetch harmonized standards published in the OJ for *directive*.

    Uses the EUR-Lex search filtered by directive number.

    Returns list of dicts with keys:
        standard_code, title, directive, ojl_reference
    """
    search_url = (
        "https://ec.europa.eu/growth/tools-databases"
        f"/harmonised-standards/{_directive_slug(directive)}_en"
    )
    try:
        response = httpx.get(search_url, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Harmonized standards fetch failed for %s: %s", directive, exc)
        return []

    try:
        return _parse_standards(response.text, directive)
    except Exception as exc:
        logger.warning("Harmonized standards parse failed for %s: %s", directive, exc)
        return []


def build_eu_block_profile() -> dict:
    """Assemble the full EU block profile for kb_block_profiles.

    Calls fetch_eu_directives, then fetch_harmonized_standards for each
    directive, and returns the JSONB-ready structure.
    """
    now = datetime.now(timezone.utc).isoformat()
    directives = fetch_eu_directives()

    all_standards: list[dict] = []
    for d in directives:
        standards = fetch_harmonized_standards(d["directive_number"])
        all_standards.extend(standards)

    return {
        "directives": directives,
        "harmonized_standards": all_standards,
        "shared_mras": [],
        "conformity_framework": {
            "marking": "CE",
            "description": "New Legislative Framework (NLF)",
            "source_url": NLF_URL,
        },
        "source_url": NLF_URL,
        "confidence": CONFIDENCE,
        "last_verified_at": now,
    }


# ------------------------------------------------------------------
# Parsers
# ------------------------------------------------------------------

def _parse_directives(html: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for row in tree.css("table tr"):
        cells = row.css("td")
        if len(cells) < 2:
            continue

        text = cells[0].text(strip=True)
        directive_number = _extract_directive_number(text)
        if not directive_number:
            continue

        link = cells[0].css_first("a")
        url = link.attributes.get("href", "") if link else ""
        title = cells[1].text(strip=True) if len(cells) > 1 else ""
        ojl_ref = cells[2].text(strip=True) if len(cells) > 2 else ""
        scope = cells[3].text(strip=True) if len(cells) > 3 else ""

        results.append({
            "directive_number": directive_number,
            "title": title,
            "ojl_reference": ojl_ref,
            "url": url,
            "scope": scope,
            "source_url": NLF_URL,
            "confidence": CONFIDENCE,
            "last_verified_at": now,
        })

    return results


def _parse_standards(html: str, directive: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for row in tree.css("table tr"):
        cells = row.css("td")
        if len(cells) < 2:
            continue

        code = cells[0].text(strip=True)
        if not code or code.lower() == "reference":
            continue

        title = cells[1].text(strip=True) if len(cells) > 1 else ""
        ojl_ref = cells[2].text(strip=True) if len(cells) > 2 else ""

        results.append({
            "standard_code": code,
            "title": title,
            "directive": directive,
            "ojl_reference": ojl_ref,
            "source_url": NLF_URL,
            "confidence": CONFIDENCE,
            "last_verified_at": now,
        })

    return results


def _extract_directive_number(text: str) -> str | None:
    """Extract a directive number like '2006/42/EC' from text."""
    import re
    match = re.search(r"\d{4}/\d+/\w+", text)
    return match.group(0) if match else None


def _directive_slug(directive: str) -> str:
    """Convert '2006/42/EC' to '2006-42-ec' for URL paths."""
    return directive.replace("/", "-").lower()
