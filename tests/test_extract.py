"""Tests for pipeline/extract.py — extraction router, structured/unstructured paths."""

import json
from unittest.mock import MagicMock, patch, call

import pytest

from pipeline.extract import (
    get_country_context,
    extract_structured_source,
    extract_unstructured_source,
    extract,
    re_extract_categories,
    _all_generic,
    _empty_result,
    RESULT_KEYS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(country_row=None, memberships=None):
    """Build a mock DB connection.

    country_row: tuple for kb_country_profiles JOIN result, or None.
    memberships: list of (org_code, membership_type) tuples.
    """
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    # First call: country profile query; second: memberships query
    if country_row is not None:
        cur.fetchone.return_value = country_row
        cur.fetchall.return_value = memberships or []
    else:
        cur.fetchone.return_value = None
        cur.fetchall.return_value = []

    return conn


def _mock_llm(extract_return=None):
    llm = MagicMock()
    if extract_return is not None:
        llm.extract_structured.return_value = extract_return
    return llm


# ---------------------------------------------------------------------------
# get_country_context
# ---------------------------------------------------------------------------

class TestGetCountryContext:

    def test_returns_context_for_eu_country(self):
        conn = _mock_conn(
            country_row=(
                "Germany", "DE",
                {"value": {"name": "Deutsches Institut für Normung", "acronym": "DIN"}},
                "EU", "European Union",
            ),
            memberships=[
                ("WTO", "full"),
                ("ILAC", "signatory"),
                ("IAF", "signatory"),
            ],
        )
        result = get_country_context("DE", conn)
        assert "Germany" in result
        assert "EU" in result
        assert "DIN" in result
        assert "ILAC MRA signatory" in result
        assert "WTO member" in result

    def test_returns_empty_for_unknown_country(self):
        conn = _mock_conn(country_row=None)
        result = get_country_context("XX", conn)
        assert result == ""

    def test_handles_no_block(self):
        conn = _mock_conn(
            country_row=("Japan", "JP", None, None, None),
            memberships=[("WTO", "full")],
        )
        result = get_country_context("JP", conn)
        assert "Japan" in result
        assert "EU" not in result
        assert "WTO member" in result

    def test_handles_no_nsb(self):
        conn = _mock_conn(
            country_row=("Japan", "JP", None, None, None),
            memberships=[],
        )
        result = get_country_context("JP", conn)
        assert "Japan" in result

    def test_handles_no_memberships(self):
        conn = _mock_conn(
            country_row=("Germany", "DE", None, "EU", "European Union"),
            memberships=[],
        )
        result = get_country_context("DE", conn)
        assert "Germany" in result
        assert "EU" in result

    def test_uses_parameterised_queries(self):
        conn = _mock_conn(country_row=None)
        get_country_context("DE", conn)
        cur = conn.cursor.return_value
        for c in cur.execute.call_args_list:
            sql = c.args[0]
            assert "%s" in sql

    def test_nsb_with_name_fallback(self):
        conn = _mock_conn(
            country_row=(
                "Japan", "JP",
                {"value": {"name": "Japanese Industrial Standards Committee"}},
                None, None,
            ),
            memberships=[],
        )
        result = get_country_context("JP", conn)
        assert "Japanese Industrial Standards Committee" in result

    def test_closes_cursor(self):
        conn = _mock_conn(country_row=None)
        get_country_context("DE", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_closes_cursor_on_error(self):
        conn = _mock_conn(country_row=None)
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            get_country_context("DE", conn)
        conn.cursor.return_value.close.assert_called_once()


# ---------------------------------------------------------------------------
# extract_structured_source
# ---------------------------------------------------------------------------

class TestExtractStructuredSource:

    def test_federal_register(self):
        raw = {
            "source_name": "Federal Register",
            "document_id": "2026-00123",
            "title": "Safety Standards for Widgets",
            "document_type": "regulation",
            "authority": "OSHA",
            "country": "US",
            "publication_date": "2026-01-15",
            "abstract": "Updates widget safety standards.",
        }
        result = extract_structured_source(raw, "federal_register")
        assert result["title"] == "Safety Standards for Widgets"
        assert result["authority"] == "OSHA"
        assert result["country"] == "US"
        assert result["effective_date"] == "2026-01-15"
        assert result["summary"] == "Updates widget safety standards."
        assert result["extraction_method"] == "structured"
        assert result["confidence"] == 0.95
        assert result["source_name"] == "Federal Register"
        assert result["document_id"] == "2026-00123"

    def test_eurlex_metadata(self):
        raw = {
            "source_name": "EUR-Lex",
            "document_id": "32006L0042",
            "title": "Machinery Directive",
            "document_type": "DIR",
            "authority": "EP",
            "country": "EU",
            "effective_date": "2009-12-29",
            "eurovoc_descriptors": ["industrial safety", "machinery"],
        }
        result = extract_structured_source(raw, "eurlex_metadata")
        assert result["title"] == "Machinery Directive"
        assert result["country"] == "EU"
        assert result["effective_date"] == "2009-12-29"
        assert result["product_categories"] == ["industrial safety", "machinery"]
        assert result["extraction_method"] == "structured"
        assert result["confidence"] == 0.95

    def test_generic_structured(self):
        raw = {
            "source_name": "Other",
            "document_id": "X-123",
            "title": "Test",
            "country": "JP",
            "confidence": 0.88,
        }
        result = extract_structured_source(raw, "structured")
        assert result["title"] == "Test"
        assert result["country"] == "JP"
        assert result["confidence"] == 0.88
        assert result["extraction_method"] == "structured"

    def test_result_has_all_keys(self):
        result = extract_structured_source({}, "federal_register")
        for key in RESULT_KEYS:
            assert key in result

    def test_defaults_country_us_for_fr(self):
        result = extract_structured_source({}, "federal_register")
        assert result["country"] == "US"

    def test_defaults_country_eu_for_eurlex(self):
        result = extract_structured_source({}, "eurlex_metadata")
        assert result["country"] == "EU"


# ---------------------------------------------------------------------------
# extract_unstructured_source
# ---------------------------------------------------------------------------

class TestExtractUnstructuredSource:

    def test_calls_llm_with_country_context(self):
        conn = _mock_conn(
            country_row=("Germany", "DE", None, "EU", "European Union"),
            memberships=[],
        )
        llm = _mock_llm({
            "title": "Machinery Safety Rule",
            "document_type": "regulation",
            "authority": "BMAS",
            "country": "DE",
            "effective_date": "2026-01-01",
            "product_categories": ["CNC machines"],
            "standards_referenced": ["EN 60204-1"],
            "confidence": 0.85,
        })

        result = extract_unstructured_source(
            "full document text", "EUR-Lex", "DE", conn, llm,
        )

        assert result["title"] == "Machinery Safety Rule"
        assert result["extraction_method"] == "llm"
        assert result["source_name"] == "EUR-Lex"
        # Verify country context was passed
        call_kwargs = llm.extract_structured.call_args
        system_ctx = call_kwargs.kwargs.get("system_context", "")
        assert "Germany" in system_ctx

    def test_returns_empty_on_llm_failure(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm()
        llm.extract_structured.side_effect = Exception("timeout")

        result = extract_unstructured_source("text", "FR", "US", conn, llm)

        assert result["title"] == ""
        assert result["extraction_method"] == "llm"

    def test_result_has_all_keys(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({"title": "Test"})

        result = extract_unstructured_source("text", "FR", "US", conn, llm)
        for key in RESULT_KEYS:
            assert key in result

    def test_coerces_non_list_categories_to_empty(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Test",
            "product_categories": "machinery",  # string not list
        })
        result = extract_unstructured_source("text", "FR", "US", conn, llm)
        assert result["product_categories"] == []

    def test_coerces_non_list_standards_to_empty(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Test",
            "standards_referenced": "EN 60204-1",
        })
        result = extract_unstructured_source("text", "FR", "US", conn, llm)
        assert result["standards_referenced"] == []

    def test_no_country_context_when_unknown(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({"title": "Test"})

        extract_unstructured_source("text", "FR", "XX", conn, llm)
        call_kwargs = llm.extract_structured.call_args
        system_ctx = call_kwargs.kwargs.get("system_context", "")
        assert "Country context" not in system_ctx


# ---------------------------------------------------------------------------
# extract (router)
# ---------------------------------------------------------------------------

class TestExtractRouter:

    def test_routes_structured_federal_register(self):
        raw = {
            "source_type": "federal_register",
            "source_name": "Federal Register",
            "document_id": "2026-00123",
            "title": "Widget Rule",
            "country": "US",
        }
        conn = MagicMock()
        llm = MagicMock()
        result = extract(raw, conn, llm)
        assert result["extraction_method"] == "structured"
        assert result["title"] == "Widget Rule"
        # LLM should not have been called
        llm.extract_structured.assert_not_called()

    def test_routes_structured_eurlex(self):
        raw = {
            "source_type": "eurlex_metadata",
            "source_name": "EUR-Lex",
            "document_id": "32006L0042",
            "title": "Machinery Directive",
            "country": "EU",
        }
        result = extract(raw, MagicMock(), MagicMock())
        assert result["extraction_method"] == "structured"

    def test_routes_unstructured(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Extracted Title",
            "product_categories": ["CNC lathes"],
            "confidence": 0.80,
        })
        raw = {
            "source_type": "unstructured",
            "source_name": "EUR-Lex",
            "document_id": "32006L0042",
            "country": "DE",
            "full_text": "Document text here...",
        }
        result = extract(raw, conn, llm)
        assert result["extraction_method"] == "llm"
        assert result["title"] == "Extracted Title"
        assert result["document_id"] == "32006L0042"

    def test_routes_unknown_source_type_as_unstructured(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({"title": "Test"})
        raw = {
            "source_type": "something_new",
            "source_name": "Other",
            "document_id": "X-1",
            "country": "JP",
            "full_text": "text",
        }
        result = extract(raw, conn, llm)
        assert result["extraction_method"] == "llm"

    def test_routes_missing_source_type_as_unstructured(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({"title": "Test"})
        raw = {
            "source_name": "Other",
            "document_id": "X-1",
            "country": "JP",
            "full_text": "text",
        }
        result = extract(raw, conn, llm)
        assert result["extraction_method"] == "llm"

    def test_shallow_categories_triggers_re_extract(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Test",
            "product_categories": ["machinery"],
            "confidence": 0.80,
        })
        # Second call (re-extract) returns specific categories
        llm.extract_structured.side_effect = [
            {"title": "Test", "product_categories": ["machinery"], "confidence": 0.80},
            {"categories": ["CNC milling machines", "hydraulic presses"]},
        ]
        raw = {
            "source_name": "EUR-Lex",
            "document_id": "32006L0042",
            "country": "DE",
            "full_text": "Document about CNC milling machines and hydraulic presses...",
        }
        result = extract(raw, conn, llm)
        assert result["product_categories"] == ["CNC milling machines", "hydraulic presses"]
        assert llm.extract_structured.call_count == 2

    def test_specific_categories_skip_re_extract(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Test",
            "product_categories": ["CNC lathes", "hydraulic presses"],
            "confidence": 0.80,
        })
        raw = {
            "source_name": "EUR-Lex",
            "document_id": "32006L0042",
            "country": "DE",
            "full_text": "text",
        }
        result = extract(raw, conn, llm)
        assert result["product_categories"] == ["CNC lathes", "hydraulic presses"]
        # Only one LLM call (no re-extract)
        assert llm.extract_structured.call_count == 1

    def test_empty_categories_skip_re_extract(self):
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({
            "title": "Test",
            "product_categories": [],
            "confidence": 0.80,
        })
        raw = {
            "source_name": "FR",
            "document_id": "X",
            "country": "US",
            "full_text": "text",
        }
        result = extract(raw, conn, llm)
        assert llm.extract_structured.call_count == 1

    def test_result_always_has_all_keys(self):
        """Both structured and unstructured paths return all keys."""
        # Structured
        structured = extract(
            {"source_type": "federal_register", "title": "T"},
            MagicMock(), MagicMock(),
        )
        for key in RESULT_KEYS:
            assert key in structured

        # Unstructured
        conn = _mock_conn(country_row=None)
        llm = _mock_llm({"title": "T"})
        unstructured = extract(
            {"source_name": "X", "document_id": "1", "country": "US", "full_text": "t"},
            conn, llm,
        )
        for key in RESULT_KEYS:
            assert key in unstructured

    def test_shallow_re_extract_logs(self):
        conn = _mock_conn(country_row=None)
        llm = MagicMock()
        llm.extract_structured.side_effect = [
            {"title": "T", "product_categories": ["equipment"], "confidence": 0.8},
            {"categories": ["boilers"]},
        ]
        raw = {
            "source_name": "X", "document_id": "1",
            "country": "US", "full_text": "text",
        }
        with patch("pipeline.extract.logger") as mock_logger:
            extract(raw, conn, llm)
            mock_logger.info.assert_any_call("shallow categories detected, re-extracting")


# ---------------------------------------------------------------------------
# re_extract_categories (preserved from earlier tests)
# ---------------------------------------------------------------------------

class TestReExtractCategories:

    def test_returns_specific_categories(self):
        llm = _mock_llm({"categories": ["fired steam boilers", "safety relief valves"]})
        existing = {"product_categories": ["pressure equipment"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["fired steam boilers", "safety relief valves"]

    def test_keeps_existing_when_still_generic(self):
        llm = _mock_llm({"categories": ["machinery", "equipment"]})
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["machinery"]

    def test_keeps_existing_when_empty_response(self):
        llm = _mock_llm({"categories": []})
        existing = {"product_categories": ["devices"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["devices"]

    def test_keeps_existing_on_llm_failure(self):
        llm = MagicMock()
        llm.extract_structured.side_effect = Exception("connection refused")
        existing = {"product_categories": ["equipment"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["equipment"]

    def test_handles_bare_list_response(self):
        llm = _mock_llm(["hydraulic presses", "centrifugal pumps"])
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["hydraulic presses", "centrifugal pumps"]

    def test_mixed_result_returns_new(self):
        llm = _mock_llm({"categories": ["machinery", "fired steam boilers"]})
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["machinery", "fired steam boilers"]

    def test_system_prompt_is_specific(self):
        llm = _mock_llm({"categories": ["boilers"]})
        re_extract_categories("doc text", {"product_categories": []}, llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.kwargs.get("system_context", "")
        assert "maximally specific" in system_ctx
        assert "Never return generic" in system_ctx

    def test_returns_empty_list_when_no_existing(self):
        llm = _mock_llm({"categories": ["machinery"]})
        result = re_extract_categories("doc text", {}, llm)
        assert result == []


# ---------------------------------------------------------------------------
# _all_generic helper
# ---------------------------------------------------------------------------

class TestAllGeneric:

    def test_all_generic_true(self):
        assert _all_generic(["machinery", "equipment"]) is True

    def test_mixed_false(self):
        assert _all_generic(["machinery", "CNC lathes"]) is False

    def test_all_specific_false(self):
        assert _all_generic(["CNC lathes", "hydraulic presses"]) is False

    def test_empty_false(self):
        assert _all_generic([]) is False

    def test_case_insensitive(self):
        assert _all_generic(["Machinery", "EQUIPMENT"]) is True

    def test_whitespace_trimmed(self):
        assert _all_generic(["  machinery  "]) is True


# ---------------------------------------------------------------------------
# _empty_result
# ---------------------------------------------------------------------------

class TestEmptyResult:

    def test_has_all_keys(self):
        result = _empty_result()
        for key in RESULT_KEYS:
            assert key in result

    def test_defaults_are_sensible(self):
        result = _empty_result()
        assert result["product_categories"] == []
        assert result["standards_referenced"] == []
        assert result["confidence"] == 0.0
        assert result["effective_date"] is None
