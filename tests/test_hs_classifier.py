"""Tests for pipeline/hs_classifier.py."""

import json
from unittest.mock import MagicMock, patch, call

import pytest

from pipeline.hs_classifier import (
    get_country_scope,
    lookup_mapping,
    get_candidates_by_keyword,
    store_new_mapping,
    queue_for_review,
    classify_regulation,
    _classify_single,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(fetchone=None, fetchall=None):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    return conn


def _mock_llm(classification=None):
    llm = MagicMock()
    llm.classify_hs_code.return_value = classification or {
        "code": "8413.70",
        "code_type": "WCO_6",
        "confidence": 0.765,
        "reasoning": "Centrifugal pumps for liquids",
        "national_variant": None,
    }
    return llm


SAMPLE_CANDIDATES = [
    ("8413.70", "WCO_6", "Centrifugal pumps for liquids"),
    ("8413.30", "WCO_6", "Fuel injection pumps"),
    ("8413.91", "WCO_6", "Parts of pumps for liquids"),
]

SAMPLE_MAPPING_ROW = ("8413.70", "WCO_6", 0.92, "Verified pump code", None)


# ===========================================================================
# get_country_scope
# ===========================================================================

class TestGetCountryScope:

    def test_us_returns_hts(self):
        conn = _mock_conn()
        assert get_country_scope("US", conn) == "US_HTS_10"

    def test_eu_country_returns_cn8(self):
        conn = _mock_conn()
        assert get_country_scope("DE", conn) == "EU_CN_8"
        assert get_country_scope("FR", conn) == "EU_CN_8"

    def test_eu_code_returns_cn8(self):
        conn = _mock_conn()
        assert get_country_scope("EU", conn) == "EU_CN_8"

    def test_non_eu_non_us_returns_wco(self):
        conn = _mock_conn(fetchone=None)
        assert get_country_scope("JP", conn) == "WCO_6"

    def test_db_eu_block_returns_cn8(self):
        conn = _mock_conn(fetchone=("EU",))
        assert get_country_scope("XX", conn) == "EU_CN_8"

    def test_db_non_eu_block_returns_wco(self):
        conn = _mock_conn(fetchone=("ASEAN",))
        assert get_country_scope("TH", conn) == "WCO_6"

    def test_uses_parameterised_query(self):
        conn = _mock_conn(fetchone=None)
        get_country_scope("JP", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_closes_cursor(self):
        conn = _mock_conn(fetchone=None)
        get_country_scope("JP", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_all_27_eu_members_return_cn8(self):
        conn = _mock_conn()
        for code in ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI",
                      "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU",
                      "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]:
            assert get_country_scope(code, conn) == "EU_CN_8", f"Failed for {code}"


# ===========================================================================
# lookup_mapping
# ===========================================================================

class TestLookupMapping:

    def test_returns_mapping_when_found(self):
        conn = _mock_conn(fetchone=SAMPLE_MAPPING_ROW)
        result = lookup_mapping("centrifugal pumps", "WCO_6", conn)
        assert result is not None
        assert result["code"] == "8413.70"
        assert result["confidence"] == 0.92

    def test_returns_none_when_not_found(self):
        conn = _mock_conn(fetchone=None)
        result = lookup_mapping("unknown product", "WCO_6", conn)
        assert result is None

    def test_uses_parameterised_query(self):
        conn = _mock_conn(fetchone=None)
        lookup_mapping("pumps", "EU_CN_8", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql
        assert "verified = TRUE" in sql

    def test_filters_by_country_scope(self):
        conn = _mock_conn(fetchone=None)
        lookup_mapping("pumps", "EU_CN_8", conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[1] == "EU_CN_8"

    def test_orders_by_confidence_desc(self):
        conn = _mock_conn(fetchone=None)
        lookup_mapping("pumps", "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ORDER BY confidence DESC" in sql

    def test_closes_cursor(self):
        conn = _mock_conn(fetchone=None)
        lookup_mapping("pumps", "WCO_6", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# get_candidates_by_keyword
# ===========================================================================

class TestGetCandidatesByKeyword:

    def test_returns_candidates(self):
        conn = _mock_conn(fetchall=SAMPLE_CANDIDATES)
        result = get_candidates_by_keyword("pumps", "WCO_6", conn)
        assert len(result) == 3
        assert result[0]["code"] == "8413.70"
        assert result[0]["description"] == "Centrifugal pumps for liquids"

    def test_returns_empty_when_no_matches(self):
        conn = _mock_conn(fetchall=[])
        result = get_candidates_by_keyword("xyzzy", "WCO_6", conn)
        assert result == []

    def test_uses_plainto_tsquery(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "plainto_tsquery" in sql
        assert "to_tsvector" in sql

    def test_uses_ts_rank_ordering(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ts_rank" in sql
        assert "DESC" in sql

    def test_limits_results(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn, limit=10)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[-1] == 10

    def test_default_limit_20(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[-1] == 20

    def test_includes_wco_fallback(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "EU_CN_8", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "'WCO'" in sql

    def test_uses_parameterised_query(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_closes_cursor(self):
        conn = _mock_conn(fetchall=[])
        get_candidates_by_keyword("pumps", "WCO_6", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# store_new_mapping
# ===========================================================================

class TestStoreNewMapping:

    def test_inserts_with_correct_values(self):
        conn = _mock_conn()
        classification = {
            "code": "8413.70",
            "code_type": "WCO_6",
            "confidence": 0.765,
            "reasoning": "Pumps",
            "national_variant": None,
        }
        store_new_mapping("centrifugal pumps", classification, "WCO_6", conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[0] == "centrifugal pumps"
        assert params[1] == "8413.70"
        assert params[7] == "opus_ingestion"
        assert params[8] is False

    def test_on_conflict_do_nothing(self):
        conn = _mock_conn()
        store_new_mapping("pumps", {"code": "1234"}, "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ON CONFLICT DO NOTHING" in sql

    def test_commits(self):
        conn = _mock_conn()
        store_new_mapping("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            store_new_mapping("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.rollback.assert_called_once()

    def test_closes_cursor(self):
        conn = _mock_conn()
        store_new_mapping("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# queue_for_review
# ===========================================================================

class TestQueueForReview:

    def test_inserts_into_validation_queue(self):
        conn = _mock_conn()
        classification = {"code": "8413.70", "confidence": 0.76}
        queue_for_review("pumps", classification, "WCO_6", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "validation_queue" in sql

    def test_issue_detail_is_json(self):
        conn = _mock_conn()
        classification = {"code": "8413.70", "confidence": 0.76, "reasoning": "OK"}
        queue_for_review("pumps", classification, "WCO_6", conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        detail = json.loads(params[2])
        assert detail["product_category"] == "pumps"
        assert detail["code"] == "8413.70"
        assert detail["country_scope"] == "WCO_6"

    def test_commits(self):
        conn = _mock_conn()
        queue_for_review("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.commit.assert_called()

    def test_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            queue_for_review("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.rollback.assert_called_once()

    def test_closes_cursor(self):
        conn = _mock_conn()
        queue_for_review("pumps", {"code": "1234"}, "WCO_6", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# classify_regulation
# ===========================================================================

class TestClassifyRegulation:

    def test_lookup_hit_skips_llm(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # get_country_scope: no block row
        # lookup_mapping: returns a verified mapping
        cur.fetchone.side_effect = [
            None,                     # get_country_scope DB check
            SAMPLE_MAPPING_ROW,       # lookup_mapping
        ]
        cur.fetchall.return_value = []
        llm = _mock_llm()

        regulation = {
            "product_categories": ["centrifugal pumps"],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)

        assert len(results) == 1
        assert results[0]["mapping_method"] == "lookup"
        assert results[0]["code"] == "8413.70"
        llm.classify_hs_code.assert_not_called()

    def test_llm_path_when_no_lookup(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # get_country_scope: no block
        # lookup_mapping: no match
        # get_candidates_by_keyword: 3 candidates
        # store_new_mapping: ok
        # queue_for_review: ok
        cur.fetchone.side_effect = [
            None,   # get_country_scope
            None,   # lookup_mapping
        ]
        cur.fetchall.return_value = SAMPLE_CANDIDATES
        llm = _mock_llm()

        regulation = {
            "product_categories": ["centrifugal pumps"],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)

        assert len(results) == 1
        assert results[0]["mapping_method"] == "llm_rag"
        assert results[0]["code"] == "8413.70"
        llm.classify_hs_code.assert_called_once()

    def test_no_candidates_returns_empty_result(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [None, None]
        cur.fetchall.return_value = []
        llm = _mock_llm()

        regulation = {
            "product_categories": ["unknown alien device"],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)

        assert len(results) == 1
        assert results[0]["mapping_method"] == "no_candidates"
        assert results[0]["code"] is None
        llm.classify_hs_code.assert_not_called()

    def test_multiple_categories(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # For each category: get_country_scope once + lookup + (candidates if no lookup)
        cur.fetchone.side_effect = [
            None,                     # get_country_scope
            SAMPLE_MAPPING_ROW,       # lookup cat 1 -> hit
            None,                     # lookup cat 2 -> miss
        ]
        cur.fetchall.return_value = SAMPLE_CANDIDATES
        llm = _mock_llm()

        regulation = {
            "product_categories": ["centrifugal pumps", "hydraulic presses"],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)

        assert len(results) == 2
        assert results[0]["mapping_method"] == "lookup"
        assert results[1]["mapping_method"] == "llm_rag"

    def test_empty_categories(self):
        conn = _mock_conn()
        llm = _mock_llm()
        regulation = {"product_categories": [], "country": "DE"}
        results = classify_regulation(regulation, conn, llm)
        assert results == []

    def test_missing_categories_key(self):
        conn = _mock_conn()
        llm = _mock_llm()
        regulation = {"country": "DE"}
        results = classify_regulation(regulation, conn, llm)
        assert results == []

    def test_skips_blank_categories(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [None]
        cur.fetchall.return_value = []
        llm = _mock_llm()

        regulation = {
            "product_categories": ["", "  "],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)
        assert results == []

    def test_eu_country_gets_cn8_scope(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [SAMPLE_MAPPING_ROW]
        llm = _mock_llm()

        regulation = {
            "product_categories": ["pumps"],
            "country": "DE",
        }
        classify_regulation(regulation, conn, llm)
        # lookup_mapping should be called with EU_CN_8
        lookup_calls = [
            c for c in cur.execute.call_args_list
            if "kb_product_hs_mappings" in str(c)
        ]
        assert any("EU_CN_8" in str(c) for c in lookup_calls)

    def test_per_category_error_isolation(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [
            None,   # get_country_scope
            None,   # lookup cat 1
            None,   # lookup cat 2
        ]
        cur.fetchall.return_value = SAMPLE_CANDIDATES
        llm = MagicMock()
        llm.classify_hs_code.side_effect = [
            Exception("LLM down"),
            {"code": "8413.70", "code_type": "WCO_6", "confidence": 0.8,
             "reasoning": "OK", "national_variant": None},
        ]

        regulation = {
            "product_categories": ["product A", "product B"],
            "country": "JP",
        }
        results = classify_regulation(regulation, conn, llm)
        # First fails (caught), second succeeds
        assert len(results) == 1
        assert results[0]["mapping_method"] == "llm_rag"

    def test_stores_and_queues_on_llm_path(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [None, None]
        cur.fetchall.return_value = SAMPLE_CANDIDATES
        llm = _mock_llm()

        regulation = {
            "product_categories": ["centrifugal pumps"],
            "country": "JP",
        }
        classify_regulation(regulation, conn, llm)

        # Should have INSERT into kb_product_hs_mappings AND validation_queue
        all_sql = [str(c) for c in cur.execute.call_args_list]
        assert any("kb_product_hs_mappings" in s for s in all_sql)
        assert any("validation_queue" in s for s in all_sql)
