"""Tests for hs_library/build_library.py."""

from unittest.mock import MagicMock, patch, call

import pytest

from hs_library.build_library import (
    build_initial_hs_mappings,
    _search_candidates,
    _insert_mapping,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(candidates=None):
    """Build a mock DB connection.

    candidates: list of tuples to return from full-text search.
    """
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchall.return_value = candidates or []
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
    (1, "8413.70", "WCO_6", "Centrifugal pumps for liquids", "WCO"),
    (2, "8413.30", "WCO_6", "Fuel injection pumps", "WCO"),
    (3, "8413.91", "WCO_6", "Parts of pumps for liquids", "WCO"),
]


# ===========================================================================
# build_initial_hs_mappings
# ===========================================================================

class TestBuildInitialHsMappings:

    def test_maps_single_category(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        result = build_initial_hs_mappings(
            ["centrifugal pumps"], "WCO", conn, llm,
        )
        assert result["mapped"] == 1
        assert result["failed"] == 0
        assert result["skipped"] == 0

    def test_maps_multiple_categories(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        result = build_initial_hs_mappings(
            ["centrifugal pumps", "hydraulic presses"], "WCO", conn, llm,
        )
        assert result["mapped"] == 2

    def test_skips_empty_category(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        result = build_initial_hs_mappings(
            ["centrifugal pumps", "", "  "], "WCO", conn, llm,
        )
        assert result["mapped"] == 1
        assert result["skipped"] == 2

    def test_skips_when_no_candidates(self):
        conn = _mock_conn(candidates=[])
        llm = _mock_llm()
        result = build_initial_hs_mappings(
            ["unknown product"], "WCO", conn, llm,
        )
        assert result["mapped"] == 0
        assert result["skipped"] == 1
        llm.classify_hs_code.assert_not_called()

    def test_handles_llm_failure(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = MagicMock()
        llm.classify_hs_code.side_effect = Exception("LLM timeout")
        result = build_initial_hs_mappings(
            ["centrifugal pumps"], "WCO", conn, llm,
        )
        assert result["mapped"] == 0
        assert result["failed"] == 1

    def test_one_failure_does_not_abort_batch(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = MagicMock()
        llm.classify_hs_code.side_effect = [
            Exception("timeout"),
            {"code": "8413.70", "code_type": "WCO_6", "confidence": 0.8,
             "reasoning": "OK", "national_variant": None},
        ]
        result = build_initial_hs_mappings(
            ["product A", "product B"], "WCO", conn, llm,
        )
        assert result["mapped"] == 1
        assert result["failed"] == 1

    def test_passes_candidates_to_llm(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        build_initial_hs_mappings(["centrifugal pumps"], "WCO", conn, llm)
        call_args = llm.classify_hs_code.call_args
        product_desc = call_args[0][0]
        candidates = call_args[0][1]
        assert product_desc == "centrifugal pumps"
        assert len(candidates) == 3
        assert candidates[0]["code"] == "8413.70"

    def test_inserts_mapping_to_db(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        build_initial_hs_mappings(["centrifugal pumps"], "WCO", conn, llm)
        cur = conn.cursor.return_value
        # Should have executed: search query + insert query
        insert_calls = [
            c for c in cur.execute.call_args_list
            if "kb_product_hs_mappings" in str(c)
        ]
        assert len(insert_calls) == 1

    def test_empty_list_returns_zeros(self):
        conn = _mock_conn()
        llm = _mock_llm()
        result = build_initial_hs_mappings([], "WCO", conn, llm)
        assert result == {"mapped": 0, "failed": 0, "skipped": 0}

    def test_strips_category_whitespace(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        llm = _mock_llm()
        build_initial_hs_mappings(["  centrifugal pumps  "], "WCO", conn, llm)
        product_desc = llm.classify_hs_code.call_args[0][0]
        assert product_desc == "centrifugal pumps"


# ===========================================================================
# _search_candidates
# ===========================================================================

class TestSearchCandidates:

    def test_returns_candidate_dicts(self):
        conn = _mock_conn(candidates=SAMPLE_CANDIDATES)
        result = _search_candidates("pumps", "WCO", conn)
        assert len(result) == 3
        assert result[0]["code"] == "8413.70"
        assert result[0]["description"] == "Centrifugal pumps for liquids"
        assert result[0]["id"] == 1

    def test_uses_parameterised_query(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "EU", conn)
        cur = conn.cursor.return_value
        sql = cur.execute.call_args.args[0]
        assert "%s" in sql
        assert "plainto_tsquery" in sql

    def test_query_uses_gin_tsquery(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "WCO", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "to_tsvector" in sql
        assert "plainto_tsquery" in sql
        assert "ts_rank" in sql

    def test_limits_to_20(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "WCO", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "LIMIT 20" in sql

    def test_filters_by_country_scope(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "EU", conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert "EU" in params

    def test_includes_wco_fallback(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "EU", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "WCO" in sql

    def test_returns_empty_when_no_matches(self):
        conn = _mock_conn(candidates=[])
        result = _search_candidates("xyzzy", "WCO", conn)
        assert result == []

    def test_closes_cursor(self):
        conn = _mock_conn(candidates=[])
        _search_candidates("pumps", "WCO", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# _insert_mapping
# ===========================================================================

class TestInsertMapping:

    def test_inserts_with_correct_values(self):
        conn = _mock_conn()
        classification = {
            "code": "8413.70",
            "code_type": "WCO_6",
            "confidence": 0.765,
            "reasoning": "Centrifugal pumps for liquids",
            "national_variant": None,
        }
        _insert_mapping("centrifugal pumps", classification, "WCO", conn)
        cur = conn.cursor.return_value
        params = cur.execute.call_args.args[1]
        assert params[0] == "centrifugal pumps"
        assert params[1] == "8413.70"
        assert params[2] == "WCO_6"
        assert params[3] == 0.765
        assert params[7] == "opus_initial"
        assert params[8] is False

    def test_uses_parameterised_query(self):
        conn = _mock_conn()
        _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_commits_on_success(self):
        conn = _mock_conn()
        _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        conn.rollback.assert_called_once()

    def test_closes_cursor(self):
        conn = _mock_conn()
        _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_closes_cursor_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_source_is_opus_initial(self):
        conn = _mock_conn()
        _insert_mapping("test", {"code": "1234"}, "WCO", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "kb_product_hs_mappings" in sql
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert "opus_initial" in params
