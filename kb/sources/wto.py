"""Fetch WTO membership data from wto.org."""

import logging
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.wto.org/english/thewto_e/countries_e/org6_map_e.htm"
CONFIDENCE = 0.95
TIMEOUT = 30


def fetch_wto_members() -> list[dict]:
    """Return WTO member records: iso2, is_member, accession_date.

    Scrapes the WTO members page.  On any HTTP or parse error the function
    logs a warning and returns an empty list — it never raises.
    """
    try:
        response = httpx.get(SOURCE_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("WTO fetch failed: %s", exc)
        return []

    try:
        return _parse_members(response.text)
    except Exception as exc:
        logger.warning("WTO parse failed: %s", exc)
        return []


def _parse_members(html: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for row in tree.css("table tr"):
        cells = row.css("td")
        if len(cells) < 3:
            continue

        country_link = cells[0].css_first("a")
        if not country_link:
            continue

        href = country_link.attributes.get("href", "")
        # WTO links contain the ISO2 code, e.g. ".../country_e/country_XX_e.htm"
        iso2 = _extract_iso2(href)
        if not iso2:
            continue

        accession_text = cells[2].text(strip=True) if len(cells) > 2 else ""
        accession_date = _parse_date(accession_text)

        results.append({
            "iso2": iso2,
            "is_member": True,
            "accession_date": accession_date,
            "source_url": SOURCE_URL,
            "confidence": CONFIDENCE,
            "last_verified_at": now,
        })

    return results


def _extract_iso2(href: str) -> str | None:
    """Pull a 2-letter country code from a WTO country page URL."""
    # Pattern: .../<something>_XX_e.htm
    parts = href.rstrip("/").split("/")
    if not parts:
        return None
    filename = parts[-1]
    segments = filename.replace(".htm", "").split("_")
    for seg in segments:
        if len(seg) == 2 and seg.isalpha():
            return seg.upper()
    return None


def _parse_date(text: str) -> str | None:
    """Best-effort parse of a date string like '1 January 1995'."""
    text = text.strip()
    if not text:
        return None
    for fmt in ("%d %B %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None
