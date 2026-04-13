"""Tests for pipeline/validate.py and pipeline/extract.py."""

from unittest.mock import MagicMock

import pytest

from pipeline.validate import validate_regulation, _extract_celex_year
from pipeline.extract import re_extract_categories


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_record(**overrides) -> dict:
    """Build a minimal valid regulation record."""
    base = {
        "title": "Test Regulation",
        "country": "EU",
        "document_type": "regulation",
    }
    base.update(overrides)
    return base


def _eurlex_source(document_id: str = "32006L0042") -> dict:
    return {"source_name": "EUR-Lex", "document_id": document_id}


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------

class TestRequiredFields:

    def test_passes_with_all_fields(self):
        result = validate_regulation(_valid_record())
        assert result["status"] == "pass"
        assert result["issues"] == []

    def test_fails_missing_title(self):
        result = validate_regulation(_valid_record(title=""))
        assert result["status"] == "quarantine"
        assert any(i["issue_type"] == "missing_field" for i in result["issues"])

    def test_fails_missing_country(self):
        result = validate_regulation(_valid_record(country=None))
        assert result["status"] == "quarantine"

    def test_fails_missing_document_type(self):
        result = validate_regulation(_valid_record(document_type=""))
        assert result["status"] == "quarantine"


# ---------------------------------------------------------------------------
# Low confidence
# ---------------------------------------------------------------------------

class TestLowConfidence:

    def test_passes_above_threshold(self):
        result = validate_regulation(_valid_record(confidence=0.8))
        assert result["status"] == "pass"

    def test_quarantines_below_threshold(self):
        result = validate_regulation(_valid_record(confidence=0.3))
        assert result["status"] == "quarantine"
        assert result["issues"][0]["issue_type"] == "low_confidence"

    def test_passes_when_no_confidence(self):
        result = validate_regulation(_valid_record())
        assert result["status"] == "pass"


# ---------------------------------------------------------------------------
# Date sanity check
# ---------------------------------------------------------------------------

class TestDateSanity:

    def test_flags_eurlex_date_matching_celex_year(self):
        record = _valid_record(effective_date="2006-06-09")
        source = _eurlex_source("32006L0042")
        result = validate_regulation(record, source)
        assert result["status"] == "quarantine"
        issues = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(issues) == 1
        assert "signing date" in issues[0]["issue_detail"]

    def test_passes_eurlex_date_not_matching_celex_year(self):
        record = _valid_record(effective_date="2009-12-29")
        source = _eurlex_source("32006L0042")
        result = validate_regulation(record, source)
        suspect = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(suspect) == 0

    def test_skips_non_eurlex_source(self):
        record = _valid_record(effective_date="2023-01-01")
        source = {"source_name": "Federal Register", "document_id": "2023-12345"}
        result = validate_regulation(record, source)
        suspect = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(suspect) == 0

    def test_skips_when_no_effective_date(self):
        source = _eurlex_source("32006L0042")
        result = validate_regulation(_valid_record(), source)
        suspect = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(suspect) == 0

    def test_skips_when_no_source(self):
        record = _valid_record(effective_date="2006-06-09")
        result = validate_regulation(record, source=None)
        suspect = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(suspect) == 0

    def test_various_celex_formats(self):
        # Regulation format: 32014R0305
        record = _valid_record(effective_date="2014-04-01")
        source = _eurlex_source("32014R0305")
        result = validate_regulation(record, source)
        suspect = [i for i in result["issues"] if i["issue_type"] == "suspect_date"]
        assert len(suspect) == 1

    def test_celex_year_extraction(self):
        assert _extract_celex_year("32006L0042") == 2006
        assert _extract_celex_year("32014R0305") == 2014
        assert _extract_celex_year("invalid") is None


# ---------------------------------------------------------------------------
# Product categories depth check
# ---------------------------------------------------------------------------

class TestCategoriesDepth:

    def test_flags_single_generic_category(self):
        record = _valid_record(product_categories=["machinery"])
        result = validate_regulation(record)
        assert result["status"] == "quarantine"
        issues = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(issues) == 1
        assert "too generic" in issues[0]["issue_detail"]

    def test_flags_two_generic_categories(self):
        record = _valid_record(product_categories=["equipment", "devices"])
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 1

    def test_passes_three_or_more_generic(self):
        record = _valid_record(
            product_categories=["machinery", "equipment", "devices"]
        )
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 0

    def test_passes_specific_categories(self):
        record = _valid_record(
            product_categories=["centrifugal pumps", "hydraulic presses"]
        )
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 0

    def test_passes_mix_generic_and_specific(self):
        record = _valid_record(
            product_categories=["machinery", "centrifugal pumps"]
        )
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 0

    def test_case_insensitive(self):
        record = _valid_record(product_categories=["Machinery"])
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 1

    def test_whitespace_trimmed(self):
        record = _valid_record(product_categories=["  equipment  "])
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 1

    def test_skips_when_no_categories(self):
        result = validate_regulation(_valid_record())
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 0

    def test_skips_empty_list(self):
        record = _valid_record(product_categories=[])
        result = validate_regulation(record)
        shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
        assert len(shallow) == 0

    def test_all_generic_terms_detected(self):
        """Each generic term should be flagged when alone."""
        for term in [
            "pressure equipment", "assemblies", "machinery", "equipment",
            "products", "goods", "articles", "devices",
        ]:
            record = _valid_record(product_categories=[term])
            result = validate_regulation(record)
            shallow = [i for i in result["issues"] if i["issue_type"] == "shallow_categories"]
            assert len(shallow) == 1, f"Failed for term: {term}"


# ---------------------------------------------------------------------------
# Integration: check ordering
# ---------------------------------------------------------------------------

class TestCheckOrdering:

    def test_date_and_categories_both_flagged(self):
        """Both new checks can fire on the same record."""
        record = _valid_record(
            effective_date="2006-06-09",
            product_categories=["equipment"],
        )
        source = _eurlex_source("32006L0042")
        result = validate_regulation(record, source)
        types = {i["issue_type"] for i in result["issues"]}
        assert "suspect_date" in types
        assert "shallow_categories" in types
        assert result["status"] == "quarantine"

    def test_valid_record_passes_all_checks(self):
        record = _valid_record(
            effective_date="2009-12-29",
            product_categories=["centrifugal pumps", "hydraulic presses"],
            confidence=0.9,
        )
        source = _eurlex_source("32006L0042")
        result = validate_regulation(record, source)
        assert result["status"] == "pass"
        assert result["issues"] == []


# ---------------------------------------------------------------------------
# re_extract_categories
# ---------------------------------------------------------------------------

class TestReExtractCategories:

    def test_returns_specific_categories(self):
        llm = MagicMock()
        llm.extract_structured.return_value = {
            "categories": ["fired steam boilers", "safety relief valves"]
        }
        existing = {"product_categories": ["pressure equipment"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["fired steam boilers", "safety relief valves"]

    def test_keeps_existing_when_still_generic(self):
        llm = MagicMock()
        llm.extract_structured.return_value = {
            "categories": ["machinery", "equipment"]
        }
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["machinery"]

    def test_keeps_existing_when_empty_response(self):
        llm = MagicMock()
        llm.extract_structured.return_value = {"categories": []}
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
        llm = MagicMock()
        llm.extract_structured.return_value = [
            "hydraulic presses", "centrifugal pumps"
        ]
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["hydraulic presses", "centrifugal pumps"]

    def test_mixed_result_returns_new(self):
        """If at least one category is specific, use the new list."""
        llm = MagicMock()
        llm.extract_structured.return_value = {
            "categories": ["machinery", "fired steam boilers"]
        }
        existing = {"product_categories": ["machinery"]}
        result = re_extract_categories("doc text", existing, llm)
        assert result == ["machinery", "fired steam boilers"]

    def test_system_prompt_is_specific(self):
        llm = MagicMock()
        llm.extract_structured.return_value = {"categories": ["boilers"]}
        re_extract_categories("doc text", {"product_categories": []}, llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("system_context", "")
        assert "maximally specific" in system_ctx
        assert "Never return generic" in system_ctx

    def test_returns_empty_list_when_no_existing(self):
        llm = MagicMock()
        llm.extract_structured.return_value = {"categories": ["machinery"]}
        existing = {}
        result = re_extract_categories("doc text", existing, llm)
        assert result == []
