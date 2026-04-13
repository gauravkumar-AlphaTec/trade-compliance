"""Tests for pipeline/validate.py (quarantine, validate_and_route)
and pipeline/load.py (upsert_source, upsert_regulation, save_hs_mappings, load).
"""

from unittest.mock import MagicMock, patch, call

import pytest

from pipeline.validate import (
    validate_regulation,
    quarantine,
    validate_and_route,
)
from pipeline.load import (
    upsert_source,
    upsert_regulation,
    save_hs_mappings,
    load,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(source_id=1, regulation_id=10):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.return_value = (source_id,)
    return conn


def _mock_conn_sequence(*fetchone_values):
    """Mock conn where successive cursor().fetchone() calls return different values."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = list(fetchone_values)
    return conn


def _valid_record(**overrides) -> dict:
    base = {
        "source_name": "EUR-Lex",
        "document_id": "32006L0042",
        "title": "Machinery Directive",
        "document_type": "directive",
        "country": "EU",
        "authority": "European Parliament",
        "effective_date": "2009-12-29",
        "full_text": "Full text of the directive.",
        "summary": "A summary.",
        "content_hash": "abc123",
    }
    base.update(overrides)
    return base


# ===========================================================================
# quarantine
# ===========================================================================

class TestQuarantine:

    def test_inserts_issues_into_validation_queue(self):
        conn = _mock_conn()
        issues = [
            "missing_field: title is missing",
            "low_confidence: confidence 0.3 below threshold",
        ]
        quarantine(_valid_record(), issues, conn)
        cur = conn.cursor.return_value
        assert cur.execute.call_count == 2
        for c in cur.execute.call_args_list:
            sql = c.args[0]
            assert "validation_queue" in sql
            assert "%s" in sql

    def test_parses_issue_type_from_text(self):
        conn = _mock_conn()
        quarantine(_valid_record(), ["missing_field: title is empty"], conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[1] == "missing_field"
        assert params[2] == "title is empty"

    def test_fallback_issue_type(self):
        conn = _mock_conn()
        quarantine(_valid_record(), ["something went wrong"], conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[1] == "validation_failure"
        assert params[2] == "something went wrong"

    def test_commits_on_success(self):
        conn = _mock_conn()
        quarantine(_valid_record(), ["issue: detail"], conn)
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            quarantine(_valid_record(), ["issue: detail"], conn)
        conn.rollback.assert_called_once()

    def test_closes_cursor(self):
        conn = _mock_conn()
        quarantine(_valid_record(), ["issue: detail"], conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_empty_issues_no_insert(self):
        conn = _mock_conn()
        quarantine(_valid_record(), [], conn)
        conn.cursor.return_value.execute.assert_not_called()
        conn.commit.assert_called_once()


# ===========================================================================
# validate_and_route
# ===========================================================================

class TestValidateAndRoute:

    def test_pass_on_valid_record(self):
        conn = _mock_conn()
        result = validate_and_route(_valid_record(), conn)
        assert result == "pass"

    def test_duplicate_on_skip_flag(self):
        conn = _mock_conn()
        record = _valid_record(_skip=True)
        result = validate_and_route(record, conn)
        assert result == "duplicate"

    def test_quarantine_missing_title(self):
        conn = _mock_conn()
        record = _valid_record(title="")
        result = validate_and_route(record, conn)
        assert result == "quarantine"
        # Should have inserted into validation_queue
        cur = conn.cursor.return_value
        assert any("validation_queue" in str(c) for c in cur.execute.call_args_list)

    def test_quarantine_missing_country(self):
        conn = _mock_conn()
        record = _valid_record(country=None)
        result = validate_and_route(record, conn)
        assert result == "quarantine"

    def test_quarantine_missing_document_type(self):
        conn = _mock_conn()
        record = _valid_record(document_type="")
        result = validate_and_route(record, conn)
        assert result == "quarantine"

    def test_quarantine_low_confidence(self):
        conn = _mock_conn()
        record = _valid_record(confidence=0.3)
        result = validate_and_route(record, conn)
        assert result == "quarantine"

    def test_quarantine_shallow_categories(self):
        conn = _mock_conn()
        record = _valid_record(product_categories=["machinery"])
        result = validate_and_route(record, conn)
        assert result == "quarantine"

    def test_quarantine_suspect_date(self):
        conn = _mock_conn()
        record = _valid_record(effective_date="2006-06-09")
        source = {"source_name": "EUR-Lex", "document_id": "32006L0042"}
        result = validate_and_route(record, conn, source=source)
        assert result == "quarantine"

    def test_pass_with_specific_categories(self):
        conn = _mock_conn()
        record = _valid_record(
            product_categories=["centrifugal pumps", "hydraulic presses"],
        )
        result = validate_and_route(record, conn)
        assert result == "pass"

    def test_pass_with_high_confidence(self):
        conn = _mock_conn()
        record = _valid_record(confidence=0.95)
        result = validate_and_route(record, conn)
        assert result == "pass"


# ===========================================================================
# upsert_source
# ===========================================================================

class TestUpsertSource:

    def test_returns_source_id(self):
        conn = _mock_conn(source_id=42)
        result = upsert_source(
            {"source_name": "EUR-Lex", "document_id": "32006L0042"},
            conn,
        )
        assert result == 42

    def test_uses_parameterised_query(self):
        conn = _mock_conn()
        upsert_source({"source_name": "FR", "document_id": "123"}, conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_on_conflict_updates(self):
        conn = _mock_conn()
        upsert_source({"source_name": "FR", "document_id": "123"}, conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql

    def test_closes_cursor(self):
        conn = _mock_conn()
        upsert_source({"source_name": "FR", "document_id": "123"}, conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_passes_content_hash(self):
        conn = _mock_conn()
        upsert_source(
            {"source_name": "FR", "document_id": "123", "content_hash": "sha256hash"},
            conn,
        )
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert "sha256hash" in params


# ===========================================================================
# upsert_regulation
# ===========================================================================

class TestUpsertRegulation:

    def test_returns_regulation_id(self):
        conn = _mock_conn()
        conn.cursor.return_value.fetchone.return_value = (99,)
        result = upsert_regulation(_valid_record(), 1, conn)
        assert result == 99

    def test_uses_parameterised_query(self):
        conn = _mock_conn()
        upsert_regulation(_valid_record(), 1, conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_on_conflict_source_id_updates(self):
        conn = _mock_conn()
        upsert_regulation(_valid_record(), 1, conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ON CONFLICT (source_id)" in sql
        assert "DO UPDATE" in sql

    def test_passes_source_id(self):
        conn = _mock_conn()
        upsert_regulation(_valid_record(), 42, conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[0] == 42

    def test_passes_all_fields(self):
        conn = _mock_conn()
        record = _valid_record()
        upsert_regulation(record, 1, conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert "Machinery Directive" in params
        assert "EU" in params

    def test_closes_cursor(self):
        conn = _mock_conn()
        upsert_regulation(_valid_record(), 1, conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_defaults_status_active(self):
        conn = _mock_conn()
        record = _valid_record()
        record.pop("status", None)
        upsert_regulation(record, 1, conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert "active" in params


# ===========================================================================
# save_hs_mappings
# ===========================================================================

class TestSaveHsMappings:

    def test_saves_mappings(self):
        conn = _mock_conn()
        mappings = [
            {"hs_code_id": 1, "confidence": 0.9, "mapping_method": "lookup"},
            {"hs_code_id": 2, "confidence": 0.8, "mapping_method": "llm_rag"},
        ]
        result = save_hs_mappings(10, mappings, conn)
        assert result == 2
        assert conn.cursor.return_value.execute.call_count == 2

    def test_skips_mapping_without_hs_code_id(self):
        conn = _mock_conn()
        mappings = [
            {"hs_code_id": 1, "confidence": 0.9},
            {"confidence": 0.5},  # no hs_code_id
        ]
        result = save_hs_mappings(10, mappings, conn)
        assert result == 1

    def test_empty_mappings_returns_zero(self):
        conn = _mock_conn()
        result = save_hs_mappings(10, [], conn)
        assert result == 0

    def test_on_conflict_updates(self):
        conn = _mock_conn()
        save_hs_mappings(10, [{"hs_code_id": 1, "confidence": 0.9}], conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql

    def test_uses_parameterised_query(self):
        conn = _mock_conn()
        save_hs_mappings(10, [{"hs_code_id": 1}], conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_passes_regulation_id(self):
        conn = _mock_conn()
        save_hs_mappings(42, [{"hs_code_id": 1}], conn)
        params = conn.cursor.return_value.execute.call_args.args[1]
        assert params[0] == 42

    def test_closes_cursor(self):
        conn = _mock_conn()
        save_hs_mappings(10, [{"hs_code_id": 1}], conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# load
# ===========================================================================

class TestLoad:

    def test_full_load_returns_ids(self):
        conn = _mock_conn_sequence((1,), (10,))
        record = _valid_record(hs_mappings=[
            {"hs_code_id": 5, "confidence": 0.85, "mapping_method": "llm_rag"},
        ])
        result = load(record, conn)
        assert result["source_id"] == 1
        assert result["regulation_id"] == 10
        assert result["mappings_saved"] == 1

    def test_commits_on_success(self):
        conn = _mock_conn_sequence((1,), (10,))
        record = _valid_record()
        load(record, conn)
        conn.commit.assert_called_once()

    def test_rollback_on_source_failure(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("source insert failed")
        with pytest.raises(Exception):
            load(_valid_record(), conn)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_rollback_on_regulation_failure(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # First call succeeds (source), second fails (regulation)
        cur.fetchone.side_effect = [(1,), Exception("regulation insert failed")]
        cur.execute.side_effect = [None, Exception("regulation insert failed")]
        with pytest.raises(Exception):
            load(_valid_record(), conn)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_rollback_on_mapping_failure(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [(1,), (10,)]
        # source + regulation succeed, mapping fails
        call_count = [0]
        def execute_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 3:  # third execute is the mapping insert
                raise Exception("mapping insert failed")
        cur.execute.side_effect = execute_side_effect

        record = _valid_record(hs_mappings=[
            {"hs_code_id": 5, "confidence": 0.85},
        ])
        with pytest.raises(Exception):
            load(record, conn)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_load_without_mappings(self):
        conn = _mock_conn_sequence((1,), (10,))
        record = _valid_record()
        result = load(record, conn)
        assert result["mappings_saved"] == 0

    def test_single_transaction(self):
        """Ensure commit is called only once at the end, not per step."""
        conn = _mock_conn_sequence((1,), (10,))
        record = _valid_record(hs_mappings=[
            {"hs_code_id": 1, "confidence": 0.9},
            {"hs_code_id": 2, "confidence": 0.8},
        ])
        load(record, conn)
        assert conn.commit.call_count == 1

    def test_uses_parameterised_queries_throughout(self):
        conn = _mock_conn_sequence((1,), (10,))
        record = _valid_record(hs_mappings=[
            {"hs_code_id": 1, "confidence": 0.9},
        ])
        load(record, conn)
        cur = conn.cursor.return_value
        for c in cur.execute.call_args_list:
            sql = c.args[0]
            assert "%s" in sql
