"""Tests for kb/load_profile.py and flows/enrich_country_profile.py."""

import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("OLLAMA_HOST", "http://test:11434")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from kb.load_profile import (
    upsert_country,
    upsert_memberships,
    upsert_standards,
    upsert_laws,
    upsert_mras,
    upsert_deviations,
    upsert_testing_protocols,
    EU_MEMBER_STATES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn():
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.fetchone.return_value = (1,)
    return conn


def _high_conf_item(**overrides):
    """Build a record with confidence >= 0.90 (auto_accept)."""
    base = {"confidence": 0.95, "source_url": "https://example.com",
            "last_verified_at": "2026-01-01T00:00:00"}
    base.update(overrides)
    return base


def _low_conf_item(**overrides):
    """Build a record with confidence < 0.70 (hold)."""
    base = {"confidence": 0.50, "source_url": "https://example.com",
            "last_verified_at": "2026-01-01T00:00:00"}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# upsert_country
# ---------------------------------------------------------------------------

class TestUpsertCountry:

    def test_returns_country_id(self):
        conn = _mock_conn()
        result = upsert_country(
            {"iso2": "DE", "iso3": "DEU", "country_name": "Germany"},
            conn,
        )
        assert result == 1
        conn.commit.assert_called()

    def test_links_eu_block(self):
        conn = _mock_conn()
        # First fetchone for EU block lookup, second for INSERT RETURNING
        conn.cursor.return_value.fetchone.side_effect = [(42,), (1,)]
        upsert_country(
            {"iso2": "DE", "iso3": "DEU", "country_name": "Germany"},
            conn,
        )
        # Should have queried for EU block
        calls = conn.cursor.return_value.execute.call_args_list
        assert any("kb_economic_blocks" in str(c) for c in calls)

    def test_no_eu_block_for_non_eu(self):
        conn = _mock_conn()
        upsert_country(
            {"iso2": "US", "iso3": "USA", "country_name": "United States"},
            conn,
        )
        calls = conn.cursor.return_value.execute.call_args_list
        assert not any("kb_economic_blocks" in str(c) for c in calls)

    def test_uses_parameterised_query(self):
        conn = _mock_conn()
        upsert_country(
            {"iso2": "JP", "iso3": "JPN", "country_name": "Japan"},
            conn,
        )
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_eu_member_list_complete(self):
        assert "DE" in EU_MEMBER_STATES
        assert "FR" in EU_MEMBER_STATES
        assert len(EU_MEMBER_STATES) == 27

    def test_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            upsert_country({"iso2": "XX", "iso3": "XXX", "country_name": "X"}, conn)
        conn.rollback.assert_called()


# ---------------------------------------------------------------------------
# upsert_memberships
# ---------------------------------------------------------------------------

class TestUpsertMemberships:

    def test_auto_accept_writes_to_db(self):
        conn = _mock_conn()
        memberships = [
            _high_conf_item(org_code="WTO", is_member=True, membership_type="full"),
        ]
        result = upsert_memberships(1, memberships, conn)
        assert result["accepted"] == 1
        assert result["queued"] == 0
        conn.commit.assert_called()

    def test_low_confidence_queued(self):
        conn = _mock_conn()
        memberships = [
            _low_conf_item(org_code="WTO", is_member=True),
        ]
        result = upsert_memberships(1, memberships, conn)
        assert result["accepted"] == 0
        assert result["queued"] == 1

    def test_mixed_confidence(self):
        conn = _mock_conn()
        memberships = [
            _high_conf_item(org_code="WTO", is_member=True),
            _low_conf_item(org_code="ISO", is_member=True),
        ]
        result = upsert_memberships(1, memberships, conn)
        assert result["accepted"] == 1
        assert result["queued"] == 1


# ---------------------------------------------------------------------------
# upsert_standards
# ---------------------------------------------------------------------------

class TestUpsertStandards:

    def test_auto_accept_writes(self):
        conn = _mock_conn()
        standards = [
            _high_conf_item(standard_code="ISO 12100", standard_name="Safety",
                            standard_type="design", accepted=True,
                            harmonization_level="full"),
        ]
        result = upsert_standards(1, standards, conn)
        assert result["accepted"] == 1

    def test_low_confidence_queued(self):
        conn = _mock_conn()
        standards = [
            _low_conf_item(standard_code="ISO 12100"),
        ]
        result = upsert_standards(1, standards, conn)
        assert result["queued"] == 1


# ---------------------------------------------------------------------------
# upsert_laws
# ---------------------------------------------------------------------------

class TestUpsertLaws:

    def test_auto_accept_writes(self):
        conn = _mock_conn()
        laws = [
            _high_conf_item(title="Product Safety Act", law_type="national_law"),
        ]
        result = upsert_laws(1, laws, conn)
        assert result["accepted"] == 1

    def test_low_confidence_queued(self):
        conn = _mock_conn()
        laws = [_low_conf_item(title="Unknown Law")]
        result = upsert_laws(1, laws, conn)
        assert result["queued"] == 1


# ---------------------------------------------------------------------------
# upsert_mras
# ---------------------------------------------------------------------------

class TestUpsertMras:

    def test_auto_accept_writes(self):
        conn = _mock_conn()
        mras = [
            _high_conf_item(mra_type="bilateral", name="EU-Japan MRA"),
        ]
        result = upsert_mras(1, mras, conn)
        assert result["accepted"] == 1


# ---------------------------------------------------------------------------
# upsert_deviations
# ---------------------------------------------------------------------------

class TestUpsertDeviations:

    def test_auto_accept_writes(self):
        conn = _mock_conn()
        devs = [
            _high_conf_item(reference_standard="EN 60204-1",
                            deviation_type="additional_requirements",
                            description="Extra grounding"),
        ]
        result = upsert_deviations(1, devs, conn)
        assert result["accepted"] == 1


# ---------------------------------------------------------------------------
# upsert_testing_protocols
# ---------------------------------------------------------------------------

class TestUpsertTestingProtocols:

    def test_auto_accept_writes(self):
        conn = _mock_conn()
        protocols = [
            _high_conf_item(protocol_name="CB Scheme", accepted=True,
                            accepted_conditionally=False),
        ]
        result = upsert_testing_protocols(1, protocols, conn)
        assert result["accepted"] == 1

    def test_low_confidence_queued(self):
        conn = _mock_conn()
        protocols = [_low_conf_item(protocol_name="CB Scheme")]
        result = upsert_testing_protocols(1, protocols, conn)
        assert result["queued"] == 1


# ---------------------------------------------------------------------------
# enrich_country_profile flow
# ---------------------------------------------------------------------------

class TestEnrichCountryProfileFlow:

    def test_flow_runs_all_tasks(self):
        import flows.enrich_country_profile as mod

        mock_llm = MagicMock()
        mock_llm.health_check.return_value = True

        tier1_data = {"memberships": []}
        qib_data = {"national_standards_body": None, "accreditation_body": None,
                     "metrology_institute": None, "legal_metrology_body": None}
        legal_data = {"laws": []}
        standards_data = {"standards": [], "protocols": []}
        insights_data = {"local_challenges": None, "confidence": 0.65,
                         "last_verified_at": "2026-01-01"}
        routed = {"auto_accept": {}, "spot_check": {}, "hold": {}}
        summary = {"accepted": 0, "queued": 0, "held": 0}

        with patch.object(mod, "LLMClient", return_value=mock_llm), \
             patch.object(mod, "fetch_tier1", return_value=tier1_data), \
             patch.object(mod, "fetch_qib", return_value=qib_data), \
             patch.object(mod, "fetch_legal", return_value=legal_data), \
             patch.object(mod, "fetch_standards", return_value=standards_data), \
             patch.object(mod, "fetch_insights", return_value=insights_data), \
             patch.object(mod, "upsert_country", return_value=1), \
             patch.object(mod, "score_and_diff", return_value=routed), \
             patch.object(mod, "apply_and_queue", return_value=summary), \
             patch.object(mod, "psycopg2") as mock_pg:

            mock_pg.connect.return_value = MagicMock()
            result = mod.enrich_country_profile.fn("DE")

            assert result == summary
            mod.fetch_tier1.assert_called_once_with("DE")
            mod.fetch_qib.assert_called_once()
            mod.fetch_legal.assert_called_once()
            mod.fetch_standards.assert_called_once()
            mod.fetch_insights.assert_called_once()
