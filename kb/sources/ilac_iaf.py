"""Fetch ILAC and IAF signatory/member data."""

import logging
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

ILAC_URL = "https://ilac.org/ilac-mra-and-signatories/"
IAF_URL = "https://iaf.nu/en/iaf-members-and-mlas/"
CONFIDENCE = 0.95
TIMEOUT = 30


def fetch_ilac_signatories() -> list[dict]:
    """Return ILAC MRA signatory records: iso2, is_signatory, scope.

    On any HTTP or parse error the function logs a warning and returns
    an empty list — it never raises.
    """
    try:
        response = httpx.get(ILAC_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("ILAC fetch failed: %s", exc)
        return []

    try:
        return _parse_signatories(response.text, source_url=ILAC_URL, org="ILAC")
    except Exception as exc:
        logger.warning("ILAC parse failed: %s", exc)
        return []


def fetch_iaf_members() -> list[dict]:
    """Return IAF MLA member records: iso2, is_signatory, scope.

    On any HTTP or parse error the function logs a warning and returns
    an empty list — it never raises.
    """
    try:
        response = httpx.get(IAF_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("IAF fetch failed: %s", exc)
        return []

    try:
        return _parse_signatories(response.text, source_url=IAF_URL, org="IAF")
    except Exception as exc:
        logger.warning("IAF parse failed: %s", exc)
        return []


def _parse_signatories(html: str, *, source_url: str, org: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for row in tree.css("table tr"):
        cells = row.css("td")
        if len(cells) < 2:
            continue

        link = cells[0].css_first("a")
        text = cells[0].text(strip=True)
        iso2 = _extract_iso2(link, text)
        if not iso2:
            continue

        scope = cells[1].text(strip=True) if len(cells) > 1 else ""

        results.append({
            "iso2": iso2,
            "is_signatory": True,
            "scope": scope,
            "org": org,
            "source_url": source_url,
            "confidence": CONFIDENCE,
            "last_verified_at": now,
        })

    return results


def _extract_iso2(link, text: str) -> str | None:
    """Best-effort extraction of ISO2 code from link or text."""
    if link is not None:
        href = link.attributes.get("href", "")
        parts = href.rstrip("/").split("/")
        for part in parts:
            cleaned = part.replace(".html", "").replace(".htm", "")
            if len(cleaned) == 2 and cleaned.isalpha():
                return cleaned.upper()
    if "(" in text and ")" in text:
        inside = text[text.rfind("(") + 1 : text.rfind(")")]
        if len(inside) == 2 and inside.isalpha():
            return inside.upper()
    return None
