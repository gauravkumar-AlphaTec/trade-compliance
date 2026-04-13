"""Fetch ISO member body data from iso.org."""

import logging
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.iso.org/members.html"
CONFIDENCE = 0.95
TIMEOUT = 30


def fetch_iso_members() -> list[dict]:
    """Return ISO member records: iso2, member_type, nsb_name, nsb_url.

    On any HTTP or parse error the function logs a warning and returns
    an empty list — it never raises.
    """
    try:
        response = httpx.get(SOURCE_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("ISO fetch failed: %s", exc)
        return []

    try:
        return _parse_members(response.text)
    except Exception as exc:
        logger.warning("ISO parse failed: %s", exc)
        return []


def _parse_members(html: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for row in tree.css("table tr"):
        cells = row.css("td")
        if len(cells) < 3:
            continue

        country_cell = cells[0]
        link = country_cell.css_first("a")
        iso2 = _extract_iso2(link)
        if not iso2:
            continue

        member_type = cells[1].text(strip=True).lower() if len(cells) > 1 else ""
        # Normalise to known types
        if "member" in member_type and "correspond" not in member_type:
            member_type = "full"
        elif "correspond" in member_type:
            member_type = "correspondent"
        elif "subscri" in member_type:
            member_type = "subscriber"

        nsb_name = cells[2].text(strip=True) if len(cells) > 2 else ""
        nsb_link = cells[2].css_first("a") if len(cells) > 2 else None
        nsb_url = nsb_link.attributes.get("href", "") if nsb_link else ""

        results.append({
            "iso2": iso2,
            "member_type": member_type,
            "nsb_name": nsb_name,
            "nsb_url": nsb_url,
            "source_url": SOURCE_URL,
            "confidence": CONFIDENCE,
            "last_verified_at": now,
        })

    return results


def _extract_iso2(link) -> str | None:
    """Extract ISO2 code from a link element."""
    if link is None:
        return None
    href = link.attributes.get("href", "")
    # Pattern: /member/XX.html
    parts = href.rstrip("/").split("/")
    for part in parts:
        cleaned = part.replace(".html", "").replace(".htm", "")
        if len(cleaned) == 2 and cleaned.isalpha():
            return cleaned.upper()
    return None
