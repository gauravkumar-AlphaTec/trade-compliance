"""Fetch BIPM member-state data from bipm.org."""

import logging
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.bipm.org/en/member-states"
CONFIDENCE = 0.95
TIMEOUT = 30


def fetch_bipm_members() -> list[dict]:
    """Return BIPM member records: iso2, is_member, membership_type.

    On any HTTP or parse error the function logs a warning and returns
    an empty list — it never raises.
    """
    try:
        response = httpx.get(SOURCE_URL, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("BIPM fetch failed: %s", exc)
        return []

    try:
        return _parse_members(response.text)
    except Exception as exc:
        logger.warning("BIPM parse failed: %s", exc)
        return []


def _parse_members(html: str) -> list[dict]:
    tree = HTMLParser(html)
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    # BIPM lists member states and associates in separate sections
    for section_type in ("member", "associate"):
        section = tree.css_first(f"#{section_type}s, .{section_type}s, [data-type='{section_type}']")
        nodes = section.css("li, tr") if section else tree.css("li")

        for node in nodes:
            link = node.css_first("a")
            text = node.text(strip=True)
            iso2 = _extract_iso2(link, text)
            if not iso2:
                continue

            membership_type = "member_state" if section_type == "member" else "associate"

            results.append({
                "iso2": iso2,
                "is_member": True,
                "membership_type": membership_type,
                "source_url": SOURCE_URL,
                "confidence": CONFIDENCE,
                "last_verified_at": now,
            })

    return results


def _extract_iso2(link, text: str) -> str | None:
    """Best-effort extraction of ISO2 code from link or text.

    Prefers the parenthesised code in the text (e.g. "Germany (DE)") because
    URL paths often contain language segments like "/en/" that are also 2-letter.
    """
    # Prefer parenthesised code like "(DE)"
    if "(" in text and ")" in text:
        inside = text[text.rfind("(") + 1 : text.rfind(")")]
        if len(inside) == 2 and inside.isalpha():
            return inside.upper()
    # Fallback: last segment of the link href
    if link is not None:
        href = link.attributes.get("href", "")
        parts = href.rstrip("/").split("/")
        if parts:
            cleaned = parts[-1].replace(".html", "").replace(".htm", "")
            if len(cleaned) == 2 and cleaned.isalpha():
                return cleaned.upper()
    return None
