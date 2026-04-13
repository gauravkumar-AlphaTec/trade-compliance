"""EUR-Lex source: Atom feed polling, SPARQL metadata, CELLAR document fetch.

All URLs are read from environment variables — never hardcoded.
"""

import logging
import os
import re
from datetime import datetime, timezone

import httpx
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

TIMEOUT = 30

# Defaults follow the public CELLAR endpoints.
# Override via env vars in production / Docker.
DEFAULT_ATOM_URL = "https://publications.europa.eu/resource/cellar/atom"
DEFAULT_SPARQL_URL = "https://publications.europa.eu/webapi/rdf/sparql"
DEFAULT_CELLAR_REST_URL = "https://publications.europa.eu/resource/cellar"


def _atom_url() -> str:
    return os.environ.get("EURLEX_ATOM_URL", DEFAULT_ATOM_URL)


def _sparql_url() -> str:
    return os.environ.get("EURLEX_SPARQL_URL", DEFAULT_SPARQL_URL)


def _cellar_rest_url() -> str:
    return os.environ.get("EURLEX_CELLAR_URL", DEFAULT_CELLAR_REST_URL)


# ------------------------------------------------------------------
# poll_atom_feed
# ------------------------------------------------------------------

def poll_atom_feed(since: datetime) -> list[dict]:
    """Poll the CELLAR Atom feed for entries modified after *since*.

    Returns a list of dicts with keys:
        celex_number, cellar_uri, title, updated
    """
    params = {
        "modifiedSince": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    try:
        response = httpx.get(
            _atom_url(),
            params=params,
            timeout=TIMEOUT,
            follow_redirects=True,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("EUR-Lex Atom feed fetch failed: %s", exc)
        return []

    try:
        return _parse_atom_feed(response.text)
    except Exception as exc:
        logger.warning("EUR-Lex Atom feed parse failed: %s", exc)
        return []


def _parse_atom_feed(xml_text: str) -> list[dict]:
    """Parse Atom XML and extract entries with CELEX numbers."""
    tree = HTMLParser(xml_text)
    entries: list[dict] = []

    for entry_node in tree.css("entry"):
        title_node = entry_node.css_first("title")
        id_node = entry_node.css_first("id")
        updated_node = entry_node.css_first("updated")

        if not id_node:
            continue

        cellar_uri = id_node.text(strip=True)
        title = title_node.text(strip=True) if title_node else ""
        updated = updated_node.text(strip=True) if updated_node else ""

        # Extract CELEX from link or title
        celex = _extract_celex(cellar_uri, title)
        if not celex:
            continue

        entries.append({
            "celex_number": celex,
            "cellar_uri": cellar_uri,
            "title": title,
            "updated": updated,
        })

    return entries


def _extract_celex(uri: str, title: str) -> str | None:
    """Extract a CELEX number like 32006L0042 from a URI or title."""
    pattern = r"\b[0-9]{1}[0-9]{4}[A-Z][0-9]{4}\b"
    for text in (uri, title):
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return None


# ------------------------------------------------------------------
# fetch_metadata
# ------------------------------------------------------------------

def fetch_metadata(celex_number: str) -> dict:
    """Query the EU Publications Office SPARQL endpoint for document metadata.

    Returns dict with keys:
        title, document_type, authority, effective_date, eurovoc_descriptors
    """
    query = _build_sparql_query(celex_number)

    try:
        response = httpx.post(
            _sparql_url(),
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("EUR-Lex SPARQL query failed for %s: %s", celex_number, exc)
        return {
            "title": None,
            "document_type": None,
            "authority": None,
            "effective_date": None,
            "eurovoc_descriptors": [],
        }

    try:
        return _parse_sparql_result(response.json())
    except Exception as exc:
        logger.warning("EUR-Lex SPARQL parse failed for %s: %s", celex_number, exc)
        return {
            "title": None,
            "document_type": None,
            "authority": None,
            "effective_date": None,
            "eurovoc_descriptors": [],
        }


def _build_sparql_query(celex_number: str) -> str:
    """Build a SPARQL query to fetch metadata for a CELEX number."""
    return f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT ?title ?docType ?authority ?dateForce ?descriptor
        WHERE {{
            ?work cdm:resource_legal_id_celex "{celex_number}" .
            OPTIONAL {{ ?work cdm:work_title ?title .
                        FILTER(LANG(?title) = "en") }}
            OPTIONAL {{ ?work cdm:resource_legal_type ?docType . }}
            OPTIONAL {{ ?work cdm:resource_legal_date_entry-into-force ?dateForce . }}
            OPTIONAL {{ ?work cdm:work_created_by_agent ?authority . }}
            OPTIONAL {{ ?work cdm:work_is_about_concept_eurovoc ?evConcept .
                        ?evConcept skos:prefLabel ?descriptor .
                        FILTER(LANG(?descriptor) = "en") }}
        }}
        LIMIT 100
    """


def _parse_sparql_result(data: dict) -> dict:
    """Parse SPARQL JSON results into a flat metadata dict."""
    bindings = data.get("results", {}).get("bindings", [])

    title = None
    document_type = None
    authority = None
    effective_date = None
    descriptors: list[str] = []

    for row in bindings:
        if not title and "title" in row:
            title = row["title"]["value"]
        if not document_type and "docType" in row:
            raw = row["docType"]["value"]
            document_type = raw.rsplit("/", 1)[-1] if "/" in raw else raw
        if not authority and "authority" in row:
            raw = row["authority"]["value"]
            authority = raw.rsplit("/", 1)[-1] if "/" in raw else raw
        if not effective_date and "dateForce" in row:
            effective_date = row["dateForce"]["value"]
        if "descriptor" in row:
            desc = row["descriptor"]["value"]
            if desc not in descriptors:
                descriptors.append(desc)

    return {
        "title": title,
        "document_type": document_type,
        "authority": authority,
        "effective_date": effective_date,
        "eurovoc_descriptors": descriptors,
    }


# ------------------------------------------------------------------
# fetch_document_content
# ------------------------------------------------------------------

def fetch_document_content(cellar_uri: str) -> str:
    """Fetch XHTML from the CELLAR REST API and return plain text.

    Strips all HTML tags.  Returns empty string on error.
    """
    url = cellar_uri
    if not url.startswith("http"):
        url = f"{_cellar_rest_url()}/{cellar_uri}"

    try:
        response = httpx.get(
            url,
            headers={"Accept": "application/xhtml+xml, text/html"},
            timeout=TIMEOUT,
            follow_redirects=True,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("CELLAR document fetch failed for %s: %s", cellar_uri, exc)
        return ""

    try:
        return _strip_html(response.text)
    except Exception as exc:
        logger.warning("CELLAR document parse failed for %s: %s", cellar_uri, exc)
        return ""


def _strip_html(html: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    tree = HTMLParser(html)
    text = tree.body.text(separator=" ") if tree.body else tree.html.text(separator=" ")
    # Collapse whitespace
    return re.sub(r"\s+", " ", text).strip()


# ------------------------------------------------------------------
# ingest_new_documents
# ------------------------------------------------------------------

def ingest_new_documents(since: datetime) -> list[dict]:
    """Orchestrate: poll feed -> fetch metadata -> fetch content.

    Returns list of structured dicts ready for the extraction stage:
        source_name, document_id, celex_number, cellar_uri,
        title, document_type, country, authority,
        effective_date, eurovoc_descriptors, full_text
    """
    entries = poll_atom_feed(since)
    documents: list[dict] = []

    for entry in entries:
        celex = entry["celex_number"]

        metadata = fetch_metadata(celex)
        content = fetch_document_content(entry["cellar_uri"])

        documents.append({
            "source_name": "EUR-Lex",
            "document_id": celex,
            "celex_number": celex,
            "cellar_uri": entry["cellar_uri"],
            "title": metadata.get("title") or entry.get("title", ""),
            "document_type": metadata.get("document_type"),
            "country": "EU",
            "authority": metadata.get("authority"),
            "effective_date": metadata.get("effective_date"),
            "eurovoc_descriptors": metadata.get("eurovoc_descriptors", []),
            "full_text": content,
        })

    logger.info("EUR-Lex ingest: %d documents since %s", len(documents), since)
    return documents
