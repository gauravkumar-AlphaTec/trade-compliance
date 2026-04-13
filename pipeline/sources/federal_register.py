"""Federal Register + eCFR source for US regulations.

All URLs are read from environment variables — never hardcoded.
"""

import logging
import os
import re
from datetime import datetime

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

TIMEOUT = 30

DEFAULT_FR_API_URL = "https://www.federalregister.gov/api/v1"
DEFAULT_ECFR_API_URL = "https://www.ecfr.gov/api/versioner/v1"


def _fr_api_url() -> str:
    return os.environ.get("FR_API_URL", DEFAULT_FR_API_URL)


def _ecfr_api_url() -> str:
    return os.environ.get("ECFR_API_URL", DEFAULT_ECFR_API_URL)


# ------------------------------------------------------------------
# fetch_new_rules
# ------------------------------------------------------------------

def fetch_new_rules(
    since: datetime,
    topics: list[str] | None = None,
) -> list[dict]:
    """Fetch RULE and PRORULE documents from the Federal Register API v1.

    Parameters
    ----------
    since : datetime
        Only return documents published on or after this date.
    topics : list[str] | None
        Optional topic filter terms (matched against FR topic slugs).

    Returns list of dicts:
        document_number, title, agency, publication_date,
        abstract, full_text_xml_url
    """
    params: dict = {
        "conditions[publication_date][gte]": since.strftime("%m/%d/%Y"),
        "conditions[type][]": ["RULE", "PRORULE"],
        "per_page": 100,
        "order": "newest",
    }
    if topics:
        params["conditions[topics][]"] = topics

    all_results: list[dict] = []
    page = 1

    while True:
        params["page"] = page
        try:
            response = httpx.get(
                f"{_fr_api_url()}/documents.json",
                params=params,
                timeout=TIMEOUT,
                follow_redirects=True,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Federal Register API fetch failed (page %d): %s", page, exc)
            break

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("Federal Register API JSON parse failed: %s", exc)
            break

        results = data.get("results", [])
        if not results:
            break

        for doc in results:
            agencies = doc.get("agencies", [])
            agency_name = agencies[0].get("name", "") if agencies else ""

            all_results.append({
                "document_number": doc.get("document_number", ""),
                "title": doc.get("title", ""),
                "agency": agency_name,
                "publication_date": doc.get("publication_date", ""),
                "abstract": doc.get("abstract", ""),
                "full_text_xml_url": doc.get("full_text_xml_url", ""),
            })

        # Check for next page
        next_url = data.get("next_page_url")
        if not next_url:
            break
        page += 1

    return all_results


# ------------------------------------------------------------------
# fetch_cfr_title
# ------------------------------------------------------------------

def fetch_cfr_title(title_number: int, date: str) -> str:
    """Fetch plain text for a CFR title from the eCFR API.

    Parameters
    ----------
    title_number : int
        CFR title number (e.g. 29 for Labor).
    date : str
        Date in YYYY-MM-DD format for the point-in-time version.

    Returns plain text content, or empty string on error.
    """
    url = f"{_ecfr_api_url()}/full/{date}/title-{title_number}.xml"

    try:
        response = httpx.get(
            url,
            timeout=TIMEOUT,
            follow_redirects=True,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "eCFR fetch failed for title %d date %s: %s",
            title_number,
            date,
            exc,
        )
        return ""

    try:
        return _strip_xml(response.text)
    except Exception as exc:
        logger.warning("eCFR parse failed for title %d: %s", title_number, exc)
        return ""


def _strip_xml(xml_text: str) -> str:
    """Remove XML/HTML tags and collapse whitespace."""
    tree = HTMLParser(xml_text)
    text = tree.body.text(separator=" ") if tree.body else tree.html.text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()


# ------------------------------------------------------------------
# ingest_new_documents
# ------------------------------------------------------------------

def ingest_new_documents(
    since: datetime,
    topics: list[str] | None = None,
) -> list[dict]:
    """Fetch new FR rules and return structured dicts for extraction.

    Returns list of dicts:
        source_name, document_id, title, document_type, country,
        authority, publication_date, abstract, full_text
    """
    rules = fetch_new_rules(since, topics=topics)
    documents: list[dict] = []

    for rule in rules:
        full_text = ""
        xml_url = rule.get("full_text_xml_url")
        if xml_url:
            full_text = _fetch_full_text_xml(xml_url)

        documents.append({
            "source_name": "Federal Register",
            "document_id": rule["document_number"],
            "title": rule["title"],
            "document_type": "regulation",
            "country": "US",
            "authority": rule["agency"],
            "publication_date": rule["publication_date"],
            "abstract": rule.get("abstract", ""),
            "full_text": full_text,
        })

    logger.info(
        "Federal Register ingest: %d documents since %s",
        len(documents),
        since,
    )
    return documents


def _fetch_full_text_xml(url: str) -> str:
    """Download and strip XML from a full_text_xml_url."""
    try:
        response = httpx.get(url, timeout=TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("FR full text fetch failed for %s: %s", url, exc)
        return ""

    try:
        return _strip_xml(response.text)
    except Exception as exc:
        logger.warning("FR full text parse failed: %s", exc)
        return ""
