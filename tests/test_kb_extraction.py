"""Tests for kb/extract_profile.py — LLMClient is fully mocked."""

import os
from unittest.mock import MagicMock

import pytest


from kb.extract_profile import (
    extract_qib,
    extract_legal_framework,
    extract_standards_deviations,
    extract_testing_protocols,
    extract_insights,
    INSIGHTS_CONFIDENCE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def llm():
    return MagicMock()


# ---------------------------------------------------------------------------
# extract_qib
# ---------------------------------------------------------------------------

class TestExtractQib:

    def test_returns_wrapped_bodies(self, llm):
        llm.extract_structured.return_value = {
            "national_standards_body": {
                "name": "DIN",
                "acronym": "DIN",
                "url": "https://www.din.de",
                "scope": "National standardization",
            },
            "accreditation_body": {
                "name": "DAkkS",
                "acronym": "DAkkS",
                "url": "https://www.dakks.de",
                "scope": "Accreditation",
            },
            "metrology_institute": {
                "name": "PTB",
                "acronym": "PTB",
                "url": "https://www.ptb.de",
                "scope": "Metrology",
            },
            "legal_metrology_body": None,
        }

        result = extract_qib("Germany", "DE", "https://example.com", "page text", llm)

        assert set(result.keys()) == {
            "national_standards_body",
            "accreditation_body",
            "metrology_institute",
            "legal_metrology_body",
        }
        nsb = result["national_standards_body"]
        assert nsb["value"]["name"] == "DIN"
        assert nsb["source_url"] == "https://example.com"
        assert nsb["confidence"] == 0.90
        assert "last_verified_at" in nsb

    def test_passes_system_context_with_country(self, llm):
        llm.extract_structured.return_value = {
            "national_standards_body": None,
            "accreditation_body": None,
            "metrology_institute": None,
            "legal_metrology_body": None,
        }
        extract_qib("Germany", "DE", "https://example.com", "text", llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("system_context", "")
        assert "Germany" in system_ctx
        assert "DE" in system_ctx

    def test_null_body_still_wrapped(self, llm):
        llm.extract_structured.return_value = {
            "national_standards_body": None,
            "accreditation_body": None,
            "metrology_institute": None,
            "legal_metrology_body": None,
        }
        result = extract_qib("Japan", "JP", "https://example.com", "text", llm)
        assert result["legal_metrology_body"]["value"] is None
        assert result["legal_metrology_body"]["confidence"] == 0.90


# ---------------------------------------------------------------------------
# extract_legal_framework
# ---------------------------------------------------------------------------

class TestExtractLegalFramework:

    def test_returns_laws_and_authorities(self, llm):
        llm.extract_structured.return_value = {
            "laws": [
                {
                    "title": "Product Safety Act",
                    "law_type": "national_law",
                    "scope": "Consumer products",
                    "url": "https://example.com/law",
                    "standards_mandatory": True,
                    "local_adaptation_notes": None,
                },
            ],
            "regulatory_authorities": [
                {
                    "name": "BAuA",
                    "acronym": "BAuA",
                    "scope": "Occupational safety",
                    "url": "https://www.baua.de",
                    "authority_type": "market_surveillance",
                },
            ],
        }

        result = extract_legal_framework("Germany", "DE", "page text", llm)

        assert len(result) == 2
        law = next(r for r in result if r["record_type"] == "law")
        assert law["title"] == "Product Safety Act"
        assert law["confidence"] == 0.80
        assert "last_verified_at" in law

        auth = next(r for r in result if r["record_type"] == "regulatory_authority")
        assert auth["name"] == "BAuA"
        assert auth["confidence"] == 0.80

    def test_empty_when_no_data(self, llm):
        llm.extract_structured.return_value = {
            "laws": [],
            "regulatory_authorities": [],
        }
        result = extract_legal_framework("Japan", "JP", "text", llm)
        assert result == []


# ---------------------------------------------------------------------------
# extract_standards_deviations
# ---------------------------------------------------------------------------

class TestExtractStandardsDeviations:

    def test_eu_member_extracts_deviations(self, llm):
        llm.extract_structured.return_value = {
            "deviations": [
                {
                    "reference_standard": "EN 60204-1",
                    "deviation_type": "additional_requirements",
                    "description": "Additional grounding requirement",
                    "documentation_required": ["test report"],
                },
            ],
            "standards_acceptance": [
                {
                    "standard_code": "EN ISO 12100",
                    "standard_name": "Safety of machinery",
                    "standard_type": "design",
                    "accepted": True,
                    "national_equivalent": "DIN EN ISO 12100",
                    "harmonization_level": "full",
                    "comments": None,
                },
            ],
        }

        result = extract_standards_deviations(
            "Germany", "DE", True, ["machinery"], "page text", llm
        )

        assert len(result) == 2
        dev = next(r for r in result if r["record_type"] == "deviation")
        assert dev["reference_standard"] == "EN 60204-1"
        assert dev["confidence"] == 0.80

        std = next(r for r in result if r["record_type"] == "standards_acceptance")
        assert std["standard_code"] == "EN ISO 12100"
        assert std["harmonization_level"] == "full"

    def test_eu_system_context_mentions_eu(self, llm):
        llm.extract_structured.return_value = {"deviations": [], "standards_acceptance": []}
        extract_standards_deviations("Germany", "DE", True, ["machinery"], "text", llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("system_context", "")
        assert "EU" in system_ctx or "deviation" in system_ctx.lower()

    def test_non_eu_uses_full_acceptance_schema(self, llm):
        llm.extract_structured.return_value = {
            "standards_acceptance": [
                {
                    "standard_code": "JIS C 8105",
                    "standard_name": "Luminaires",
                    "standard_type": "testing",
                    "accepted": True,
                    "national_equivalent": "JIS C 8105",
                    "harmonization_level": "partial",
                    "comments": "Modified adoption",
                },
            ],
        }

        result = extract_standards_deviations(
            "Japan", "JP", False, ["lighting"], "page text", llm
        )

        assert len(result) == 1
        assert result[0]["record_type"] == "standards_acceptance"
        assert result[0]["standard_code"] == "JIS C 8105"

    def test_non_eu_system_context_no_eu_mention(self, llm):
        llm.extract_structured.return_value = {"standards_acceptance": []}
        extract_standards_deviations("Japan", "JP", False, [], "text", llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("system_context", "")
        assert "Japan" in system_ctx

    def test_empty_product_categories(self, llm):
        llm.extract_structured.return_value = {"standards_acceptance": []}
        extract_standards_deviations("Japan", "JP", False, [], "text", llm)
        call_args = llm.extract_structured.call_args
        system_ctx = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("system_context", "")
        assert "general" in system_ctx


# ---------------------------------------------------------------------------
# extract_testing_protocols
# ---------------------------------------------------------------------------

class TestExtractTestingProtocols:

    def test_returns_protocols(self, llm):
        llm.extract_structured.return_value = {
            "protocols": [
                {
                    "protocol_name": "CB Scheme",
                    "accepted": True,
                    "accepted_conditionally": False,
                    "conditions": [],
                    "notes": None,
                },
                {
                    "protocol_name": "IECEx",
                    "accepted": True,
                    "accepted_conditionally": True,
                    "conditions": [
                        {"type": "scope", "description": "Only for Zone 1/2"}
                    ],
                    "notes": "Limited to hazardous areas",
                },
            ],
        }

        result = extract_testing_protocols("Germany", "DE", "page text", llm)

        assert len(result) == 2
        cb = result[0]
        assert cb["protocol_name"] == "CB Scheme"
        assert cb["accepted"] is True
        assert cb["confidence"] == 0.80
        assert "last_verified_at" in cb

        iecex = result[1]
        assert iecex["accepted_conditionally"] is True
        assert len(iecex["conditions"]) == 1

    def test_empty_when_no_protocols(self, llm):
        llm.extract_structured.return_value = {"protocols": []}
        result = extract_testing_protocols("Japan", "JP", "text", llm)
        assert result == []


# ---------------------------------------------------------------------------
# extract_insights
# ---------------------------------------------------------------------------

class TestExtractInsights:

    def test_returns_insights_with_forced_confidence(self, llm):
        llm.extract_structured.return_value = {
            "local_challenges": "Complex bureaucratic processes",
            "recent_reforms": "New digital customs portal launched 2025",
            "useful_portals": [
                {
                    "name": "Zoll Online",
                    "url": "https://www.zoll.de",
                    "description": "German customs portal",
                }
            ],
            "regulatory_deadlines": [
                {"description": "New CE marking rules", "date": "2026-01-01"}
            ],
            "general_notes": "Strong enforcement culture",
        }

        result = extract_insights("Germany", "DE", llm)

        assert result["local_challenges"] == "Complex bureaucratic processes"
        assert result["confidence"] == INSIGHTS_CONFIDENCE
        assert result["confidence"] == 0.65
        assert "last_verified_at" in result
        assert len(result["useful_portals"]) == 1

    def test_no_page_content_passed(self, llm):
        """Insights should use model knowledge, not a web page."""
        llm.extract_structured.return_value = {
            "local_challenges": None,
            "recent_reforms": None,
            "useful_portals": [],
            "regulatory_deadlines": [],
            "general_notes": None,
        }
        extract_insights("Germany", "DE", llm)

        call_args = llm.extract_structured.call_args
        text_arg = call_args.args[0]
        # The text should be a prompt, not page HTML
        assert "Based on your knowledge" in text_arg

    def test_confidence_always_0_65(self, llm):
        """Even if the model returns high-quality data, confidence stays 0.65."""
        llm.extract_structured.return_value = {
            "local_challenges": "Well-documented",
            "recent_reforms": "Major reform",
            "useful_portals": [{"name": "a", "url": "b", "description": "c"}] * 10,
            "regulatory_deadlines": [],
            "general_notes": "Detailed notes",
        }
        result = extract_insights("Germany", "DE", llm)
        assert result["confidence"] == 0.65
