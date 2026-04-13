"""Tests for pipeline/sources/eurlex.py and pipeline/sources/federal_register.py."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from pipeline.sources.eurlex import (
    poll_atom_feed,
    fetch_metadata,
    fetch_document_content,
    ingest_new_documents as eurlex_ingest,
    _extract_celex,
    _parse_sparql_result,
    _strip_html,
)
from pipeline.sources.federal_register import (
    fetch_new_rules,
    fetch_cfr_title,
    ingest_new_documents as fr_ingest,
    _strip_xml,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SINCE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _mock_response(status_code=200, text="", json_data=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    else:
        resp.json.return_value = {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp,
        )
    return resp


ATOM_XML = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Directive 2006/42/EC on machinery</title>
    <id>http://publications.europa.eu/resource/cellar/abc123</id>
    <updated>2026-01-15T10:00:00Z</updated>
  </entry>
  <entry>
    <title>Regulation 32014R0305 on construction products</title>
    <id>http://publications.europa.eu/resource/cellar/def456</id>
    <updated>2026-01-16T12:00:00Z</updated>
  </entry>
  <entry>
    <title>Some other document without CELEX</title>
    <id>http://example.com/no-celex</id>
    <updated>2026-01-17T08:00:00Z</updated>
  </entry>
</feed>
"""

SPARQL_RESULT = {
    "results": {
        "bindings": [
            {
                "title": {"value": "Machinery Directive"},
                "docType": {"value": "http://example.org/type/DIR"},
                "authority": {"value": "http://example.org/agent/EP"},
                "dateForce": {"value": "2009-12-29"},
                "descriptor": {"value": "industrial safety"},
            },
            {
                "descriptor": {"value": "machinery"},
            },
        ]
    }
}

XHTML_DOC = """<html><body>
<h1>Article 1</h1>
<p>This regulation applies to   all   machinery.</p>
<p>It entered into force on 29 December 2009.</p>
</body></html>"""


# ===========================================================================
# EUR-Lex: poll_atom_feed
# ===========================================================================

class TestPollAtomFeed:

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_returns_entries_with_celex(self, mock_get):
        mock_get.return_value = _mock_response(text=ATOM_XML)
        result = poll_atom_feed(SINCE)
        celex_nums = [e["celex_number"] for e in result]
        assert "32014R0305" in celex_nums

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_entries_have_required_keys(self, mock_get):
        mock_get.return_value = _mock_response(text=ATOM_XML)
        result = poll_atom_feed(SINCE)
        for entry in result:
            assert "celex_number" in entry
            assert "cellar_uri" in entry
            assert "title" in entry
            assert "updated" in entry

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_passes_modified_since_param(self, mock_get):
        mock_get.return_value = _mock_response(text=ATOM_XML)
        poll_atom_feed(SINCE)
        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        assert params["modifiedSince"] == "2026-01-01T00:00:00Z"

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response(status_code=500)
        result = poll_atom_feed(SINCE)
        assert result == []

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        result = poll_atom_feed(SINCE)
        assert result == []

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_skips_entries_without_celex(self, mock_get):
        xml = """<feed><entry>
            <title>No CELEX here</title>
            <id>http://example.com/nothing</id>
        </entry></feed>"""
        mock_get.return_value = _mock_response(text=xml)
        result = poll_atom_feed(SINCE)
        assert result == []


# ===========================================================================
# EUR-Lex: fetch_metadata
# ===========================================================================

class TestFetchMetadata:

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_returns_parsed_metadata(self, mock_post):
        mock_post.return_value = _mock_response(json_data=SPARQL_RESULT)
        result = fetch_metadata("32006L0042")
        assert result["title"] == "Machinery Directive"
        assert result["document_type"] == "DIR"
        assert result["authority"] == "EP"
        assert result["effective_date"] == "2009-12-29"
        assert "industrial safety" in result["eurovoc_descriptors"]
        assert "machinery" in result["eurovoc_descriptors"]

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_deduplicates_descriptors(self, mock_post):
        data = {"results": {"bindings": [
            {"descriptor": {"value": "safety"}},
            {"descriptor": {"value": "safety"}},
            {"descriptor": {"value": "health"}},
        ]}}
        mock_post.return_value = _mock_response(json_data=data)
        result = fetch_metadata("32006L0042")
        assert result["eurovoc_descriptors"] == ["safety", "health"]

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_http_error_returns_empty_metadata(self, mock_post):
        mock_post.return_value = _mock_response(status_code=500)
        result = fetch_metadata("32006L0042")
        assert result["title"] is None
        assert result["eurovoc_descriptors"] == []

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_connection_error_returns_empty_metadata(self, mock_post):
        mock_post.side_effect = httpx.ConnectError("refused")
        result = fetch_metadata("32006L0042")
        assert result["title"] is None

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_empty_bindings_returns_nones(self, mock_post):
        mock_post.return_value = _mock_response(
            json_data={"results": {"bindings": []}}
        )
        result = fetch_metadata("32006L0042")
        assert result["title"] is None
        assert result["document_type"] is None
        assert result["effective_date"] is None

    @patch("pipeline.sources.eurlex.httpx.post")
    def test_sparql_query_contains_celex(self, mock_post):
        mock_post.return_value = _mock_response(json_data=SPARQL_RESULT)
        fetch_metadata("32006L0042")
        call_kwargs = mock_post.call_args
        query = call_kwargs.kwargs.get("data", {}).get("query", "")
        assert "32006L0042" in query


# ===========================================================================
# EUR-Lex: fetch_document_content
# ===========================================================================

class TestFetchDocumentContent:

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_returns_stripped_text(self, mock_get):
        mock_get.return_value = _mock_response(text=XHTML_DOC)
        result = fetch_document_content("http://publications.europa.eu/resource/cellar/abc123")
        assert "Article 1" in result
        assert "all machinery" in result
        assert "<h1>" not in result
        assert "<p>" not in result

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_collapses_whitespace(self, mock_get):
        mock_get.return_value = _mock_response(text=XHTML_DOC)
        result = fetch_document_content("http://example.com/cellar/abc")
        assert "  " not in result

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response(status_code=404)
        result = fetch_document_content("http://example.com/cellar/abc")
        assert result == ""

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        result = fetch_document_content("http://example.com/cellar/abc")
        assert result == ""

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_relative_uri_gets_base_url(self, mock_get):
        mock_get.return_value = _mock_response(text="<html><body>text</body></html>")
        fetch_document_content("abc123")
        url_called = mock_get.call_args[0][0]
        assert url_called.startswith("http")
        assert "abc123" in url_called

    @patch("pipeline.sources.eurlex.httpx.get")
    def test_accepts_xhtml_content_type(self, mock_get):
        mock_get.return_value = _mock_response(text="<html><body>ok</body></html>")
        fetch_document_content("http://example.com/doc")
        headers = mock_get.call_args.kwargs.get("headers", {})
        assert "xhtml" in headers.get("Accept", "").lower()


# ===========================================================================
# EUR-Lex: ingest_new_documents
# ===========================================================================

class TestEurlexIngest:

    @patch("pipeline.sources.eurlex.fetch_document_content", return_value="doc text")
    @patch("pipeline.sources.eurlex.fetch_metadata")
    @patch("pipeline.sources.eurlex.poll_atom_feed")
    def test_orchestrates_all_steps(self, mock_poll, mock_meta, mock_content):
        mock_poll.return_value = [
            {"celex_number": "32006L0042", "cellar_uri": "http://cellar/abc",
             "title": "Machinery Directive", "updated": "2026-01-15"},
        ]
        mock_meta.return_value = {
            "title": "Machinery Directive",
            "document_type": "DIR",
            "authority": "EP",
            "effective_date": "2009-12-29",
            "eurovoc_descriptors": ["safety"],
        }

        result = eurlex_ingest(SINCE)

        assert len(result) == 1
        doc = result[0]
        assert doc["source_name"] == "EUR-Lex"
        assert doc["document_id"] == "32006L0042"
        assert doc["country"] == "EU"
        assert doc["full_text"] == "doc text"
        assert doc["effective_date"] == "2009-12-29"

    @patch("pipeline.sources.eurlex.fetch_document_content", return_value="")
    @patch("pipeline.sources.eurlex.fetch_metadata")
    @patch("pipeline.sources.eurlex.poll_atom_feed")
    def test_uses_entry_title_as_fallback(self, mock_poll, mock_meta, mock_content):
        mock_poll.return_value = [
            {"celex_number": "32006L0042", "cellar_uri": "http://cellar/abc",
             "title": "Feed Title", "updated": "2026-01-15"},
        ]
        mock_meta.return_value = {
            "title": None,
            "document_type": None,
            "authority": None,
            "effective_date": None,
            "eurovoc_descriptors": [],
        }

        result = eurlex_ingest(SINCE)
        assert result[0]["title"] == "Feed Title"

    @patch("pipeline.sources.eurlex.poll_atom_feed", return_value=[])
    def test_empty_feed_returns_empty(self, mock_poll):
        result = eurlex_ingest(SINCE)
        assert result == []


# ===========================================================================
# EUR-Lex: helper functions
# ===========================================================================

class TestEurlexHelpers:

    def test_extract_celex_from_uri(self):
        assert _extract_celex("http://cellar/32006L0042/stuff", "") == "32006L0042"

    def test_extract_celex_from_title(self):
        assert _extract_celex("http://no-celex", "Regulation 32014R0305") == "32014R0305"

    def test_extract_celex_none_when_absent(self):
        assert _extract_celex("http://example.com", "No number here") is None

    def test_strip_html_removes_tags(self):
        assert "<p>" not in _strip_html("<p>hello</p>")

    def test_strip_html_collapses_whitespace(self):
        result = _strip_html("<p>  a   b  </p>")
        assert "  " not in result

    def test_parse_sparql_strips_uri_prefix(self):
        data = {"results": {"bindings": [
            {"docType": {"value": "http://example.org/type/REGULATION"}},
        ]}}
        result = _parse_sparql_result(data)
        assert result["document_type"] == "REGULATION"


# ===========================================================================
# Federal Register: fetch_new_rules
# ===========================================================================

FR_API_RESPONSE = {
    "results": [
        {
            "document_number": "2026-00123",
            "title": "Safety Standards for Widgets",
            "agencies": [{"name": "OSHA"}],
            "publication_date": "2026-01-15",
            "abstract": "This rule updates widget safety standards.",
            "full_text_xml_url": "https://fr.gov/xml/2026-00123.xml",
        },
        {
            "document_number": "2026-00456",
            "title": "Import Requirements Update",
            "agencies": [{"name": "CPSC"}],
            "publication_date": "2026-01-16",
            "abstract": "Updated import requirements.",
            "full_text_xml_url": "https://fr.gov/xml/2026-00456.xml",
        },
    ],
    "next_page_url": None,
}


class TestFetchNewRules:

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_returns_rules(self, mock_get):
        mock_get.return_value = _mock_response(json_data=FR_API_RESPONSE)
        result = fetch_new_rules(SINCE)
        assert len(result) == 2
        assert result[0]["document_number"] == "2026-00123"
        assert result[0]["agency"] == "OSHA"

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_result_has_required_keys(self, mock_get):
        mock_get.return_value = _mock_response(json_data=FR_API_RESPONSE)
        result = fetch_new_rules(SINCE)
        for doc in result:
            assert "document_number" in doc
            assert "title" in doc
            assert "agency" in doc
            assert "publication_date" in doc
            assert "abstract" in doc
            assert "full_text_xml_url" in doc

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_passes_date_and_type_params(self, mock_get):
        mock_get.return_value = _mock_response(json_data=FR_API_RESPONSE)
        fetch_new_rules(SINCE)
        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        assert params["conditions[publication_date][gte]"] == "01/01/2026"
        assert "RULE" in params["conditions[type][]"]
        assert "PRORULE" in params["conditions[type][]"]

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_passes_topics_when_provided(self, mock_get):
        mock_get.return_value = _mock_response(json_data=FR_API_RESPONSE)
        fetch_new_rules(SINCE, topics=["safety", "import"])
        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        assert params["conditions[topics][]"] == ["safety", "import"]

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response(status_code=500)
        result = fetch_new_rules(SINCE)
        assert result == []

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        result = fetch_new_rules(SINCE)
        assert result == []

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_handles_empty_agencies(self, mock_get):
        data = {"results": [{
            "document_number": "2026-99999",
            "title": "Test",
            "agencies": [],
            "publication_date": "2026-01-01",
            "abstract": "",
            "full_text_xml_url": "",
        }], "next_page_url": None}
        mock_get.return_value = _mock_response(json_data=data)
        result = fetch_new_rules(SINCE)
        assert result[0]["agency"] == ""

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_paginates(self, mock_get):
        page1 = {
            "results": [{"document_number": "2026-001", "title": "A",
                         "agencies": [], "publication_date": "2026-01-01",
                         "abstract": "", "full_text_xml_url": ""}],
            "next_page_url": "https://fr.gov/api/v1/documents.json?page=2",
        }
        page2 = {
            "results": [{"document_number": "2026-002", "title": "B",
                         "agencies": [], "publication_date": "2026-01-02",
                         "abstract": "", "full_text_xml_url": ""}],
            "next_page_url": None,
        }
        mock_get.side_effect = [
            _mock_response(json_data=page1),
            _mock_response(json_data=page2),
        ]
        result = fetch_new_rules(SINCE)
        assert len(result) == 2
        assert result[0]["document_number"] == "2026-001"
        assert result[1]["document_number"] == "2026-002"


# ===========================================================================
# Federal Register: fetch_cfr_title
# ===========================================================================

class TestFetchCfrTitle:

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_returns_stripped_text(self, mock_get):
        xml = "<body><section><p>Part 1910 — Safety</p></section></body>"
        mock_get.return_value = _mock_response(text=xml)
        result = fetch_cfr_title(29, "2026-01-01")
        assert "Part 1910" in result
        assert "<p>" not in result

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_builds_correct_url(self, mock_get):
        mock_get.return_value = _mock_response(text="<body>ok</body>")
        fetch_cfr_title(29, "2026-01-01")
        url_called = mock_get.call_args[0][0]
        assert "title-29" in url_called
        assert "2026-01-01" in url_called

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response(status_code=500)
        result = fetch_cfr_title(29, "2026-01-01")
        assert result == ""

    @patch("pipeline.sources.federal_register.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        result = fetch_cfr_title(29, "2026-01-01")
        assert result == ""


# ===========================================================================
# Federal Register: ingest_new_documents
# ===========================================================================

class TestFrIngest:

    @patch("pipeline.sources.federal_register._fetch_full_text_xml", return_value="rule text")
    @patch("pipeline.sources.federal_register.fetch_new_rules")
    def test_orchestrates_rules(self, mock_rules, mock_xml):
        mock_rules.return_value = [
            {
                "document_number": "2026-00123",
                "title": "Safety Standards",
                "agency": "OSHA",
                "publication_date": "2026-01-15",
                "abstract": "Updates safety standards.",
                "full_text_xml_url": "https://fr.gov/xml/2026-00123.xml",
            },
        ]

        result = fr_ingest(SINCE)

        assert len(result) == 1
        doc = result[0]
        assert doc["source_name"] == "Federal Register"
        assert doc["document_id"] == "2026-00123"
        assert doc["country"] == "US"
        assert doc["document_type"] == "regulation"
        assert doc["full_text"] == "rule text"

    @patch("pipeline.sources.federal_register.fetch_new_rules", return_value=[])
    def test_empty_rules_returns_empty(self, mock_rules):
        result = fr_ingest(SINCE)
        assert result == []

    @patch("pipeline.sources.federal_register._fetch_full_text_xml", return_value="")
    @patch("pipeline.sources.federal_register.fetch_new_rules")
    def test_missing_xml_still_returns_doc(self, mock_rules, mock_xml):
        mock_rules.return_value = [
            {
                "document_number": "2026-00789",
                "title": "Test Rule",
                "agency": "EPA",
                "publication_date": "2026-02-01",
                "abstract": "",
                "full_text_xml_url": "",
            },
        ]
        result = fr_ingest(SINCE)
        assert len(result) == 1
        assert result[0]["full_text"] == ""

    @patch("pipeline.sources.federal_register._fetch_full_text_xml", return_value="")
    @patch("pipeline.sources.federal_register.fetch_new_rules")
    def test_passes_topics_through(self, mock_rules, mock_xml):
        mock_rules.return_value = []
        fr_ingest(SINCE, topics=["safety"])
        mock_rules.assert_called_once_with(SINCE, topics=["safety"])


# ===========================================================================
# Federal Register: helper functions
# ===========================================================================

class TestFrHelpers:

    def test_strip_xml_removes_tags(self):
        result = _strip_xml("<root><p>hello</p></root>")
        assert "<p>" not in result
        assert "hello" in result

    def test_strip_xml_collapses_whitespace(self):
        result = _strip_xml("<p>  a   b  </p>")
        assert "  " not in result
