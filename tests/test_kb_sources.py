"""Tests for kb/sources/ — all HTTP calls are mocked."""

from unittest.mock import patch, MagicMock

import httpx
import pytest

from kb.sources.wto import fetch_wto_members, SOURCE_URL as WTO_URL
from kb.sources.iso_members import fetch_iso_members, SOURCE_URL as ISO_URL
from kb.sources.bipm import fetch_bipm_members, SOURCE_URL as BIPM_URL
from kb.sources.ilac_iaf import (
    fetch_ilac_signatories,
    fetch_iaf_members,
    ILAC_URL,
    IAF_URL,
)


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
# WTO
# ---------------------------------------------------------------------------

WTO_HTML = """
<html><body><table>
<tr><th>Country</th><th>Status</th><th>Date</th></tr>
<tr>
  <td><a href="/english/thewto_e/countries_e/country_de_e.htm">Germany</a></td>
  <td>Member</td>
  <td>1 January 1995</td>
</tr>
<tr>
  <td><a href="/english/thewto_e/countries_e/country_jp_e.htm">Japan</a></td>
  <td>Member</td>
  <td>1 January 1995</td>
</tr>
</table></body></html>
"""


class TestWTO:

    @patch("kb.sources.wto.httpx.get")
    def test_returns_members(self, mock_get):
        mock_get.return_value = _mock_response(WTO_HTML)
        results = fetch_wto_members()
        assert len(results) == 2
        de = results[0]
        assert de["iso2"] == "DE"
        assert de["is_member"] is True
        assert de["accession_date"] == "1995-01-01"
        assert de["confidence"] == 0.95
        assert de["source_url"] == WTO_URL

    @patch("kb.sources.wto.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        assert fetch_wto_members() == []

    @patch("kb.sources.wto.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("connection refused")
        assert fetch_wto_members() == []


# ---------------------------------------------------------------------------
# ISO
# ---------------------------------------------------------------------------

ISO_HTML = """
<html><body><table>
<tr><th>Country</th><th>Type</th><th>NSB</th></tr>
<tr>
  <td><a href="/member/DE.html">Germany</a></td>
  <td>Member body</td>
  <td><a href="https://www.din.de">DIN</a></td>
</tr>
<tr>
  <td><a href="/member/US.html">United States</a></td>
  <td>Member body</td>
  <td><a href="https://www.ansi.org">ANSI</a></td>
</tr>
</table></body></html>
"""


class TestISO:

    @patch("kb.sources.iso_members.httpx.get")
    def test_returns_members(self, mock_get):
        mock_get.return_value = _mock_response(ISO_HTML)
        results = fetch_iso_members()
        assert len(results) == 2
        de = results[0]
        assert de["iso2"] == "DE"
        assert de["member_type"] == "full"
        assert de["nsb_name"] == "DIN"
        assert de["nsb_url"] == "https://www.din.de"
        assert de["confidence"] == 0.95

    @patch("kb.sources.iso_members.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=503)
        assert fetch_iso_members() == []

    @patch("kb.sources.iso_members.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("timeout")
        assert fetch_iso_members() == []


# ---------------------------------------------------------------------------
# BIPM
# ---------------------------------------------------------------------------

BIPM_HTML = """
<html><body>
<div id="members">
  <li><a href="/en/states/DE">Germany (DE)</a></li>
  <li><a href="/en/states/FR">France (FR)</a></li>
</div>
<div id="associates">
  <li><a href="/en/states/KZ">Kazakhstan (KZ)</a></li>
</div>
</body></html>
"""


class TestBIPM:

    @patch("kb.sources.bipm.httpx.get")
    def test_returns_members(self, mock_get):
        mock_get.return_value = _mock_response(BIPM_HTML)
        results = fetch_bipm_members()
        assert len(results) >= 2
        iso2s = [r["iso2"] for r in results]
        assert "DE" in iso2s
        assert "FR" in iso2s
        de = next(r for r in results if r["iso2"] == "DE")
        assert de["is_member"] is True
        assert de["membership_type"] == "member_state"
        assert de["confidence"] == 0.95

    @patch("kb.sources.bipm.httpx.get")
    def test_associates_tagged(self, mock_get):
        mock_get.return_value = _mock_response(BIPM_HTML)
        results = fetch_bipm_members()
        kz = [r for r in results if r["iso2"] == "KZ"]
        if kz:
            assert kz[0]["membership_type"] == "associate"

    @patch("kb.sources.bipm.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        assert fetch_bipm_members() == []

    @patch("kb.sources.bipm.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        assert fetch_bipm_members() == []


# ---------------------------------------------------------------------------
# ILAC
# ---------------------------------------------------------------------------

ILAC_HTML = """
<html><body><table>
<tr><th>Economy</th><th>Scope</th></tr>
<tr>
  <td><a href="/signatory/DE">Germany (DE)</a></td>
  <td>Testing, Calibration</td>
</tr>
<tr>
  <td><a href="/signatory/JP">Japan (JP)</a></td>
  <td>Testing</td>
</tr>
</table></body></html>
"""


class TestILAC:

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_returns_signatories(self, mock_get):
        mock_get.return_value = _mock_response(ILAC_HTML)
        results = fetch_ilac_signatories()
        assert len(results) == 2
        de = results[0]
        assert de["iso2"] == "DE"
        assert de["is_signatory"] is True
        assert de["scope"] == "Testing, Calibration"
        assert de["org"] == "ILAC"
        assert de["confidence"] == 0.95

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        assert fetch_ilac_signatories() == []

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        assert fetch_ilac_signatories() == []


# ---------------------------------------------------------------------------
# IAF
# ---------------------------------------------------------------------------

IAF_HTML = """
<html><body><table>
<tr><th>Economy</th><th>Scope</th></tr>
<tr>
  <td><a href="/member/DE">Germany (DE)</a></td>
  <td>QMS, EMS</td>
</tr>
</table></body></html>
"""


class TestIAF:

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_returns_members(self, mock_get):
        mock_get.return_value = _mock_response(IAF_HTML)
        results = fetch_iaf_members()
        assert len(results) == 1
        de = results[0]
        assert de["iso2"] == "DE"
        assert de["is_signatory"] is True
        assert de["scope"] == "QMS, EMS"
        assert de["org"] == "IAF"
        assert de["confidence"] == 0.95

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        assert fetch_iaf_members() == []

    @patch("kb.sources.ilac_iaf.httpx.get")
    def test_connection_error_returns_empty(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("refused")
        assert fetch_iaf_members() == []
