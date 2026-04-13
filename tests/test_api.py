"""Tests for all API endpoints.

Uses mocked DB connections and LLMClient to test endpoint logic
without a real database.
"""

import types
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app
import api.deps as deps


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def mock_llm():
    llm = MagicMock()
    return llm


@pytest.fixture()
def mock_conn():
    """Return a mock connection with a mock cursor using RealDictCursor-style rows."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


@pytest.fixture()
def client(mock_conn, mock_llm):
    """TestClient that overrides DB and LLM dependencies."""
    def _get_db_override():
        yield mock_conn

    def _get_llm_override():
        return mock_llm

    app.dependency_overrides[deps.get_db] = _get_db_override
    app.dependency_overrides[deps.get_llm] = _get_llm_override

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


# ------------------------------------------------------------------
# Health endpoint
# ------------------------------------------------------------------

class TestHealth:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data


# ------------------------------------------------------------------
# GET /regulations
# ------------------------------------------------------------------

class TestListRegulations:
    def test_list_empty(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 0}
        cur.fetchall.return_value = []

        resp = client.get("/regulations")
        assert resp.status_code == 200
        data = resp.json()
        assert data["regulations"] == []
        assert data["total"] == 0

    def test_list_with_results(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 1}
        cur.fetchall.return_value = [
            {
                "id": 1,
                "title": "Test Regulation",
                "document_type": "RULE",
                "authority": "EPA",
                "country": "US",
                "effective_date": "2024-01-01",
                "status": "active",
            }
        ]

        resp = client.get("/regulations")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["regulations"]) == 1
        assert data["regulations"][0]["title"] == "Test Regulation"
        assert data["total"] == 1

    def test_list_filter_by_country(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 0}
        cur.fetchall.return_value = []

        resp = client.get("/regulations?country=DE")
        assert resp.status_code == 200
        # Verify the SQL included country filter
        sql = cur.execute.call_args_list[0][0][0]
        assert "r.country = %s" in sql

    def test_list_filter_by_document_type(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 0}
        cur.fetchall.return_value = []

        resp = client.get("/regulations?document_type=RULE")
        assert resp.status_code == 200
        sql = cur.execute.call_args_list[0][0][0]
        assert "r.document_type = %s" in sql

    def test_list_fulltext_search(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 0}
        cur.fetchall.return_value = []

        resp = client.get("/regulations?q=chemical")
        assert resp.status_code == 200
        sql = cur.execute.call_args_list[0][0][0]
        assert "plainto_tsquery" in sql

    def test_list_pagination(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 100}
        cur.fetchall.return_value = []

        resp = client.get("/regulations?limit=10&offset=20")
        assert resp.status_code == 200
        params = cur.execute.call_args_list[1][0][1]
        assert 10 in params
        assert 20 in params

    def test_list_combined_filters(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"cnt": 0}
        cur.fetchall.return_value = []

        resp = client.get("/regulations?country=US&document_type=RULE&q=safety")
        assert resp.status_code == 200
        sql = cur.execute.call_args_list[0][0][0]
        assert "r.country = %s" in sql
        assert "r.document_type = %s" in sql
        assert "plainto_tsquery" in sql


# ------------------------------------------------------------------
# GET /regulations/{id}
# ------------------------------------------------------------------

class TestGetRegulation:
    def _make_row(self):
        return {
            "id": 1,
            "title": "Test Regulation",
            "document_type": "RULE",
            "authority": "EPA",
            "country": "US",
            "effective_date": "2024-01-01",
            "expiry_date": None,
            "summary": "A test regulation summary",
            "status": "active",
            "created_at": "2024-01-01T00:00:00",
            "updated_at": "2024-01-01T00:00:00",
            "source_name": "Federal Register",
            "document_id": "FR-2024-001",
            "url": "https://example.com",
            "fetched_at": "2024-01-01T00:00:00",
        }

    def test_get_regulation_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        reg_row = self._make_row()
        # First call: regulation + source JOIN
        # Second call: HS codes
        # Third call in _build_country_context: country profile
        # Fourth call in _build_country_context: memberships
        cur.fetchone.side_effect = [
            reg_row,
            {"iso2": "US", "country_name": "United States", "block_code": None, "block_name": None},
        ]
        cur.fetchall.side_effect = [
            [
                {"code": "8471.30", "code_type": "WCO_6", "description": "Laptops", "confidence": 0.92},
            ],
            [],  # memberships
        ]

        resp = client.get("/regulations/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == 1
        assert data["title"] == "Test Regulation"
        assert data["source"]["source_name"] == "Federal Register"
        assert data["country_context"]["iso2"] == "US"
        assert len(data["hs_codes"]) == 1
        assert data["hs_codes"][0]["code"] == "8471.30"

    def test_get_regulation_not_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        resp = client.get("/regulations/999")
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Regulation not found"

    def test_get_regulation_no_hs_codes(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        reg_row = self._make_row()
        cur.fetchone.side_effect = [
            reg_row,
            {"iso2": "US", "country_name": "United States", "block_code": None, "block_name": None},
        ]
        cur.fetchall.side_effect = [
            [],  # no HS codes
            [],  # no memberships
        ]

        resp = client.get("/regulations/1")
        assert resp.status_code == 200
        assert resp.json()["hs_codes"] == []

    def test_get_regulation_country_not_in_kb(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        reg_row = self._make_row()
        reg_row["country"] = "ZZ"
        cur.fetchone.side_effect = [
            reg_row,
            None,  # country not found in KB
        ]
        cur.fetchall.side_effect = [
            [],  # HS codes
        ]

        resp = client.get("/regulations/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["country_context"]["iso2"] == "ZZ"
        assert data["country_context"]["country_name"] == "ZZ"


# ------------------------------------------------------------------
# GET /countries/{code}
# ------------------------------------------------------------------

class TestGetCountryProfile:
    def test_country_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "iso2": "DE",
            "iso3": "DEU",
            "country_name": "Germany",
            "region": "Europe",
            "block_code": "EU",
            "block_name": "European Union",
            "national_standards_body": {"acronym": "DIN"},
            "accreditation_body": {"acronym": "DAkkS"},
        }
        cur.fetchall.return_value = [
            {"org_code": "WTO", "is_member": True, "membership_type": "full"},
            {"org_code": "ISO", "is_member": True, "membership_type": "full"},
        ]

        resp = client.get("/countries/DE")
        assert resp.status_code == 200
        data = resp.json()
        assert data["iso2"] == "DE"
        assert data["country_name"] == "Germany"
        assert data["block_code"] == "EU"
        assert len(data["memberships"]) == 2
        assert data["memberships"][0]["org_code"] == "WTO"
        assert data["standards_body"] == {"acronym": "DIN"}

    def test_country_not_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        resp = client.get("/countries/ZZ")
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Country not found"

    def test_country_no_memberships(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "iso2": "XX",
            "iso3": "XXX",
            "country_name": "Test Country",
            "region": None,
            "block_code": None,
            "block_name": None,
            "national_standards_body": None,
            "accreditation_body": None,
        }
        cur.fetchall.return_value = []

        resp = client.get("/countries/XX")
        assert resp.status_code == 200
        data = resp.json()
        assert data["memberships"] == []

    def test_country_no_block(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "iso2": "US",
            "iso3": "USA",
            "country_name": "United States",
            "region": "Americas",
            "block_code": None,
            "block_name": None,
            "national_standards_body": {"acronym": "ANSI"},
            "accreditation_body": None,
        }
        cur.fetchall.return_value = []

        resp = client.get("/countries/US")
        assert resp.status_code == 200
        data = resp.json()
        assert data["block_code"] is None


# ------------------------------------------------------------------
# POST /hs-codes/search
# ------------------------------------------------------------------

class TestHsCodeSearch:
    def test_search_lookup_hit(self, client, mock_conn):
        """Verified mapping found — no LLM call needed."""
        with patch("api.routes.hs_codes.lookup_mapping") as mock_lookup:
            mock_lookup.return_value = {
                "code": "8471.30",
                "code_type": "WCO_6",
                "confidence": 0.95,
                "reasoning": "Direct match",
                "national_variant": None,
            }

            resp = client.post(
                "/hs-codes/search",
                json={"product_description": "laptop computers", "country_scope": "WCO"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["classification"]["code"] == "8471.30"
            assert data["classification"]["confidence"] == 0.95
            assert data["candidates"] == []

    def test_search_llm_classification(self, client, mock_conn, mock_llm):
        """No verified mapping — GIN search + LLM classification."""
        with patch("api.routes.hs_codes.lookup_mapping") as mock_lookup, \
             patch("api.routes.hs_codes.get_candidates_by_keyword") as mock_candidates:
            mock_lookup.return_value = None
            mock_candidates.return_value = [
                {"code": "8471.30", "code_type": "WCO_6", "description": "Portable computers"},
                {"code": "8471.41", "code_type": "WCO_6", "description": "Desktop computers"},
            ]
            mock_llm.classify_hs_code.return_value = {
                "code": "8471.30",
                "code_type": "WCO_6",
                "confidence": 0.88,
                "reasoning": "Laptop is a portable computer",
            }

            resp = client.post(
                "/hs-codes/search",
                json={"product_description": "laptop computers"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["classification"]["code"] == "8471.30"
            assert data["classification"]["confidence"] == 0.88
            assert len(data["candidates"]) == 2

    def test_search_no_candidates(self, client, mock_conn):
        """No candidates found in the HS code library."""
        with patch("api.routes.hs_codes.lookup_mapping") as mock_lookup, \
             patch("api.routes.hs_codes.get_candidates_by_keyword") as mock_candidates:
            mock_lookup.return_value = None
            mock_candidates.return_value = []

            resp = client.post(
                "/hs-codes/search",
                json={"product_description": "exotic alien artifact"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["classification"]["confidence"] == 0.0
            assert "No candidates" in data["classification"]["reasoning"]

    def test_search_empty_description(self, client):
        resp = client.post(
            "/hs-codes/search",
            json={"product_description": "   "},
        )
        assert resp.status_code == 400

    def test_search_default_country_scope(self, client, mock_conn):
        """Default country_scope is WCO."""
        with patch("api.routes.hs_codes.lookup_mapping") as mock_lookup:
            mock_lookup.return_value = {
                "code": "0101.21",
                "code_type": "WCO_6",
                "confidence": 0.90,
                "reasoning": "match",
                "national_variant": None,
            }

            resp = client.post(
                "/hs-codes/search",
                json={"product_description": "horses"},
            )
            assert resp.status_code == 200
            mock_lookup.assert_called_once_with("horses", "WCO", mock_conn)


# ------------------------------------------------------------------
# POST /hs-codes/compliance-check
# ------------------------------------------------------------------

class TestComplianceCheck:
    def _setup_country_context(self, cur):
        """Set up mock returns for country context queries."""
        cur.fetchone.side_effect = [
            # Country profile
            {
                "iso2": "DE",
                "country_name": "Germany",
                "block_code": "EU",
                "block_name": "European Union",
            },
        ]

    def test_compliance_check_with_hs_match(self, client, mock_conn, mock_llm):
        cur = mock_conn.cursor.return_value

        with patch("api.routes.hs_codes.get_country_scope") as mock_scope, \
             patch("api.routes.hs_codes.lookup_mapping") as mock_lookup:
            mock_scope.return_value = "EU_CN_8"
            mock_lookup.return_value = {
                "code": "8471.30.00",
                "code_type": "EU_CN_8",
                "confidence": 0.92,
                "reasoning": "Laptop computers",
                "national_variant": None,
            }

            cur.fetchone.side_effect = [
                {
                    "iso2": "DE",
                    "country_name": "Germany",
                    "block_code": "EU",
                    "block_name": "European Union",
                },
            ]
            cur.fetchall.side_effect = [
                [{"org_code": "WTO"}, {"org_code": "ISO"}],  # memberships
                [  # regulations matching HS code
                    {
                        "id": 1,
                        "title": "EU RoHS Directive",
                        "document_type": "DIRECTIVE",
                        "authority": "EU Commission",
                        "country": "DE",
                        "effective_date": "2024-01-01",
                        "status": "active",
                    },
                ],
                [{"standard_name": "EN 60950"}],  # standards
            ]

            resp = client.post(
                "/hs-codes/compliance-check",
                json={"country_code": "DE", "product_description": "laptop computers"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["country_context"]["iso2"] == "DE"
            assert data["country_context"]["block_code"] == "EU"
            assert data["hs_classification"]["code"] == "8471.30.00"
            assert len(data["regulations"]) == 1
            assert data["regulations"][0]["title"] == "EU RoHS Directive"
            assert data["standards"] == ["EN 60950"]

    def test_compliance_check_country_not_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        resp = client.post(
            "/hs-codes/compliance-check",
            json={"country_code": "ZZ", "product_description": "widgets"},
        )
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Country not found"

    def test_compliance_check_no_hs_classification(self, client, mock_conn, mock_llm):
        cur = mock_conn.cursor.return_value

        with patch("api.routes.hs_codes.get_country_scope") as mock_scope, \
             patch("api.routes.hs_codes.lookup_mapping") as mock_lookup, \
             patch("api.routes.hs_codes.get_candidates_by_keyword") as mock_candidates:
            mock_scope.return_value = "WCO_6"
            mock_lookup.return_value = None
            mock_candidates.return_value = []

            cur.fetchone.side_effect = [
                {
                    "iso2": "US",
                    "country_name": "United States",
                    "block_code": None,
                    "block_name": None,
                },
            ]
            cur.fetchall.side_effect = [
                [],  # memberships
                [  # regulations (general, no HS filter)
                    {
                        "id": 2,
                        "title": "CPSC General",
                        "document_type": "RULE",
                        "authority": "CPSC",
                        "country": "US",
                        "effective_date": "2024-06-01",
                        "status": "active",
                    },
                ],
                [],  # standards
            ]

            resp = client.post(
                "/hs-codes/compliance-check",
                json={"country_code": "US", "product_description": "mystery item"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["hs_classification"] is None
            assert len(data["regulations"]) == 1

    def test_compliance_check_empty_fields(self, client):
        resp = client.post(
            "/hs-codes/compliance-check",
            json={"country_code": "", "product_description": "test"},
        )
        assert resp.status_code == 400

    def test_compliance_check_llm_classification(self, client, mock_conn, mock_llm):
        cur = mock_conn.cursor.return_value

        with patch("api.routes.hs_codes.get_country_scope") as mock_scope, \
             patch("api.routes.hs_codes.lookup_mapping") as mock_lookup, \
             patch("api.routes.hs_codes.get_candidates_by_keyword") as mock_candidates:
            mock_scope.return_value = "US_HTS_10"
            mock_lookup.return_value = None
            mock_candidates.return_value = [
                {"code": "9503.00", "code_type": "WCO_6", "description": "Toys"},
            ]
            mock_llm.classify_hs_code.return_value = {
                "code": "9503.00.00",
                "code_type": "US_HTS_10",
                "confidence": 0.82,
                "reasoning": "Toy classification",
            }

            cur.fetchone.side_effect = [
                {
                    "iso2": "US",
                    "country_name": "United States",
                    "block_code": None,
                    "block_name": None,
                },
            ]
            cur.fetchall.side_effect = [
                [],  # memberships
                [],  # regulations
                [],  # standards
            ]

            resp = client.post(
                "/hs-codes/compliance-check",
                json={"country_code": "US", "product_description": "toy cars"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["hs_classification"]["code"] == "9503.00.00"
            assert data["hs_classification"]["confidence"] == 0.82
            mock_llm.classify_hs_code.assert_called_once()
