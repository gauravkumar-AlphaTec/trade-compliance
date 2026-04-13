"""Tests for kb/sources/eu_block.py and flows/enrich_block_profile.py."""

import json
import os
from unittest.mock import MagicMock, patch, call

import httpx
import pytest

from kb.sources.eu_block import (
    fetch_eu_directives,
    fetch_harmonized_standards,
    build_eu_block_profile,
    NLF_URL,
    CONFIDENCE,
)

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(html: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = html
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


# ---------------------------------------------------------------------------
# Sample HTML
# ---------------------------------------------------------------------------

DIRECTIVES_HTML = """
<html><body><table>
<tr><th>Directive</th><th>Title</th><th>OJ Ref</th><th>Scope</th></tr>
<tr>
  <td><a href="https://eur-lex.europa.eu/2006/42/EC">2006/42/EC</a></td>
  <td>Machinery Directive</td>
  <td>OJ L 157</td>
  <td>Machinery and partly completed machinery</td>
</tr>
<tr>
  <td><a href="https://eur-lex.europa.eu/2014/35/EU">2014/35/EU</a></td>
  <td>Low Voltage Directive</td>
  <td>OJ L 96</td>
  <td>Electrical equipment within voltage limits</td>
</tr>
</table></body></html>
"""

STANDARDS_HTML = """
<html><body><table>
<tr><th>Reference</th><th>Title</th><th>OJ Reference</th></tr>
<tr>
  <td>EN ISO 12100:2010</td>
  <td>Safety of machinery — General principles</td>
  <td>OJ C 110</td>
</tr>
<tr>
  <td>EN 60204-1:2018</td>
  <td>Safety of machinery — Electrical equipment</td>
  <td>OJ C 92</td>
</tr>
</table></body></html>
"""


# ---------------------------------------------------------------------------
# fetch_eu_directives
# ---------------------------------------------------------------------------

class TestFetchEuDirectives:

    @patch("kb.sources.eu_block.httpx.get")
    def test_returns_directives(self, mock_get):
        mock_get.return_value = _mock_response(DIRECTIVES_HTML)
        results = fetch_eu_directives()
        assert len(results) == 2
        d = results[0]
        assert d["directive_number"] == "2006/42/EC"
        assert d["title"] == "Machinery Directive"
        assert d["ojl_reference"] == "OJ L 157"
        assert d["scope"] == "Machinery and partly completed machinery"
        assert d["confidence"] == CONFIDENCE
        assert d["source_url"] == NLF_URL

    @patch("kb.sources.eu_block.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        assert fetch_eu_directives() == []

    @patch("kb.sources.eu_block.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        assert fetch_eu_directives() == []


# ---------------------------------------------------------------------------
# fetch_harmonized_standards
# ---------------------------------------------------------------------------

class TestFetchHarmonizedStandards:

    @patch("kb.sources.eu_block.httpx.get")
    def test_returns_standards(self, mock_get):
        mock_get.return_value = _mock_response(STANDARDS_HTML)
        results = fetch_harmonized_standards("2006/42/EC")
        assert len(results) == 2
        s = results[0]
        assert s["standard_code"] == "EN ISO 12100:2010"
        assert s["title"] == "Safety of machinery — General principles"
        assert s["directive"] == "2006/42/EC"
        assert s["ojl_reference"] == "OJ C 110"
        assert s["confidence"] == CONFIDENCE

    @patch("kb.sources.eu_block.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=404)
        assert fetch_harmonized_standards("2006/42/EC") == []

    @patch("kb.sources.eu_block.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("timeout")
        assert fetch_harmonized_standards("2006/42/EC") == []


# ---------------------------------------------------------------------------
# build_eu_block_profile
# ---------------------------------------------------------------------------

class TestBuildEuBlockProfile:

    @patch("kb.sources.eu_block.fetch_harmonized_standards")
    @patch("kb.sources.eu_block.fetch_eu_directives")
    def test_assembles_profile(self, mock_dirs, mock_stds):
        mock_dirs.return_value = [
            {
                "directive_number": "2006/42/EC",
                "title": "Machinery",
                "ojl_reference": "OJ L 157",
                "url": "",
                "scope": "Machinery",
                "source_url": NLF_URL,
                "confidence": CONFIDENCE,
                "last_verified_at": "2026-01-01T00:00:00",
            },
        ]
        mock_stds.return_value = [
            {
                "standard_code": "EN ISO 12100:2010",
                "title": "Safety of machinery",
                "directive": "2006/42/EC",
                "ojl_reference": "OJ C 110",
                "source_url": NLF_URL,
                "confidence": CONFIDENCE,
                "last_verified_at": "2026-01-01T00:00:00",
            },
        ]

        profile = build_eu_block_profile()

        assert len(profile["directives"]) == 1
        assert len(profile["harmonized_standards"]) == 1
        assert profile["conformity_framework"]["marking"] == "CE"
        assert profile["confidence"] == CONFIDENCE
        mock_stds.assert_called_once_with("2006/42/EC")

    @patch("kb.sources.eu_block.fetch_harmonized_standards")
    @patch("kb.sources.eu_block.fetch_eu_directives")
    def test_empty_when_no_directives(self, mock_dirs, mock_stds):
        mock_dirs.return_value = []
        profile = build_eu_block_profile()
        assert profile["directives"] == []
        assert profile["harmonized_standards"] == []
        mock_stds.assert_not_called()


# ---------------------------------------------------------------------------
# enrich_eu_block flow
# ---------------------------------------------------------------------------

class TestEnrichEuBlockFlow:

    def test_flow_calls_upsert(self):
        import flows.enrich_block_profile as mod

        profile = {
            "directives": [{"directive_number": "2006/42/EC"}],
            "harmonized_standards": [{"standard_code": "EN ISO 12100:2010"}],
            "shared_mras": [],
            "conformity_framework": {"marking": "CE"},
            "source_url": NLF_URL,
            "confidence": CONFIDENCE,
            "last_verified_at": "2026-01-01",
        }

        with patch.object(mod, "fetch_profile", return_value=profile) as mock_fetch, \
             patch.object(mod, "upsert_eu_block", return_value=None) as mock_upsert:
            mod.enrich_eu_block.fn()

        mock_fetch.assert_called_once()
        mock_upsert.assert_called_once_with(profile)
