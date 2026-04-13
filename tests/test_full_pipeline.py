"""Tests for flows/full_pipeline.py, flows/ingest_eurlex.py, flows/ingest_us.py."""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("OLLAMA_HOST", "http://test:11434")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import flows.full_pipeline as full_mod
import flows.ingest_eurlex as eurlex_mod
import flows.ingest_us as us_mod

from flows.full_pipeline import (
    get_last_run,
    update_last_run,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SINCE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _mock_conn(last_run=None):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    if last_run:
        cur.fetchone.return_value = (last_run,)
    else:
        cur.fetchone.return_value = None
    return conn


# ===========================================================================
# get_last_run / update_last_run
# ===========================================================================

class TestPipelineState:

    def test_get_last_run_returns_timestamp(self):
        conn = _mock_conn(last_run=SINCE)
        result = get_last_run("full_pipeline", conn)
        assert result == SINCE

    def test_get_last_run_default_on_missing(self):
        conn = _mock_conn(last_run=None)
        result = get_last_run("full_pipeline", conn)
        assert result.year == 2020

    def test_get_last_run_adds_utc_if_naive(self):
        naive = datetime(2026, 1, 1)
        conn = _mock_conn(last_run=naive)
        result = get_last_run("full_pipeline", conn)
        assert result.tzinfo is not None

    def test_get_last_run_uses_parameterised_query(self):
        conn = _mock_conn(last_run=None)
        get_last_run("full_pipeline", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_get_last_run_closes_cursor(self):
        conn = _mock_conn(last_run=None)
        get_last_run("test", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_update_last_run_commits(self):
        conn = _mock_conn()
        update_last_run("full_pipeline", conn)
        conn.commit.assert_called_once()

    def test_update_last_run_uses_parameterised_query(self):
        conn = _mock_conn()
        update_last_run("full_pipeline", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_update_last_run_on_conflict_updates(self):
        conn = _mock_conn()
        update_last_run("full_pipeline", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "ON CONFLICT" in sql

    def test_update_last_run_rollback_on_error(self):
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            update_last_run("full_pipeline", conn)
        conn.rollback.assert_called_once()

    def test_update_last_run_closes_cursor(self):
        conn = _mock_conn()
        update_last_run("test", conn)
        conn.cursor.return_value.close.assert_called_once()


# ===========================================================================
# Full pipeline flow
# ===========================================================================

class TestTradeCompliancePipeline:

    def test_flow_runs_all_stages(self):
        mock_llm = MagicMock()
        mock_llm.health_check.return_value = True

        eurlex_docs = [{"document_id": "32006L0042", "source_name": "EUR-Lex"}]
        us_docs = [{"document_id": "2026-001", "source_name": "Federal Register"}]
        extracted = [{"title": "T", "country": "EU", "document_type": "reg"}]
        processed = [{"title": "T", "country": "EU", "document_type": "reg"}]
        classified = [{"title": "T", "country": "EU", "document_type": "reg",
                       "hs_mappings": []}]

        with patch.object(full_mod, "LLMClient", return_value=mock_llm), \
             patch.object(full_mod, "psycopg2") as mock_pg, \
             patch.object(full_mod, "get_last_run", return_value=SINCE), \
             patch.object(full_mod, "update_last_run"), \
             patch.object(full_mod, "ingest_eurlex", return_value=eurlex_docs), \
             patch.object(full_mod, "ingest_us", return_value=us_docs), \
             patch.object(full_mod, "extract_all", return_value=extracted), \
             patch.object(full_mod, "process_all", return_value=processed), \
             patch.object(full_mod, "classify_all", return_value=classified), \
             patch.object(full_mod, "validate_all", return_value=classified), \
             patch.object(full_mod, "load_all", return_value=1):

            mock_pg.connect.return_value = MagicMock()
            result = full_mod.trade_compliance_pipeline.fn()

            assert result["ingested"] == 2
            assert result["extracted"] == 1
            assert result["processed"] == 1
            assert result["classified"] == 1
            assert result["validated"] == 1
            assert result["loaded"] == 1

            full_mod.ingest_eurlex.assert_called_once()
            full_mod.ingest_us.assert_called_once()
            full_mod.extract_all.assert_called_once()
            full_mod.process_all.assert_called_once()
            full_mod.classify_all.assert_called_once()
            full_mod.validate_all.assert_called_once()
            full_mod.load_all.assert_called_once()
            full_mod.update_last_run.assert_called_once()

    def test_quarantined_count(self):
        mock_llm = MagicMock()

        classified = [
            {"title": "A", "country": "EU", "document_type": "reg"},
            {"title": "B", "country": "EU", "document_type": "reg"},
            {"title": "C", "country": "EU", "document_type": "reg"},
        ]
        # Only 1 passes validation
        validated = [classified[0]]

        with patch.object(full_mod, "LLMClient", return_value=mock_llm), \
             patch.object(full_mod, "psycopg2") as mock_pg, \
             patch.object(full_mod, "get_last_run", return_value=SINCE), \
             patch.object(full_mod, "update_last_run"), \
             patch.object(full_mod, "ingest_eurlex", return_value=[]), \
             patch.object(full_mod, "ingest_us", return_value=[]), \
             patch.object(full_mod, "extract_all", return_value=classified), \
             patch.object(full_mod, "process_all", return_value=classified), \
             patch.object(full_mod, "classify_all", return_value=classified), \
             patch.object(full_mod, "validate_all", return_value=validated), \
             patch.object(full_mod, "load_all", return_value=1):

            mock_pg.connect.return_value = MagicMock()
            result = full_mod.trade_compliance_pipeline.fn()
            assert result["quarantined"] == 2

    def test_closes_connection(self):
        mock_llm = MagicMock()
        mock_conn = MagicMock()

        with patch.object(full_mod, "LLMClient", return_value=mock_llm), \
             patch.object(full_mod, "psycopg2") as mock_pg, \
             patch.object(full_mod, "get_last_run", return_value=SINCE), \
             patch.object(full_mod, "update_last_run"), \
             patch.object(full_mod, "ingest_eurlex", return_value=[]), \
             patch.object(full_mod, "ingest_us", return_value=[]), \
             patch.object(full_mod, "extract_all", return_value=[]), \
             patch.object(full_mod, "process_all", return_value=[]), \
             patch.object(full_mod, "classify_all", return_value=[]), \
             patch.object(full_mod, "validate_all", return_value=[]), \
             patch.object(full_mod, "load_all", return_value=0):

            mock_pg.connect.return_value = mock_conn
            full_mod.trade_compliance_pipeline.fn()
            mock_conn.close.assert_called_once()


# ===========================================================================
# Standalone EUR-Lex flow
# ===========================================================================

class TestIngestEurlexFlow:

    def test_flow_processes_documents(self):
        mock_llm = MagicMock()
        raw_docs = [
            {"document_id": "32006L0042", "source_name": "EUR-Lex",
             "full_text": "text"},
        ]

        with patch.object(eurlex_mod, "LLMClient", return_value=mock_llm), \
             patch.object(eurlex_mod, "psycopg2") as mock_pg, \
             patch.object(eurlex_mod, "fetch_eurlex", return_value=raw_docs), \
             patch.object(eurlex_mod, "extract_doc", return_value={
                 "title": "T", "country": "EU", "document_type": "directive",
             }), \
             patch.object(eurlex_mod, "process_doc", return_value={
                 "title": "T", "country": "EU", "document_type": "directive",
             }), \
             patch.object(eurlex_mod, "validate_and_load", return_value="pass"):

            mock_pg.connect.return_value = MagicMock()
            result = eurlex_mod.ingest_eurlex_flow.fn(since=SINCE)

            assert result["source"] == "EUR-Lex"
            assert result["ingested"] == 1
            assert result["loaded"] == 1
            assert result["quarantined"] == 0

    def test_quarantined_docs_counted(self):
        mock_llm = MagicMock()

        with patch.object(eurlex_mod, "LLMClient", return_value=mock_llm), \
             patch.object(eurlex_mod, "psycopg2") as mock_pg, \
             patch.object(eurlex_mod, "fetch_eurlex", return_value=[
                 {"document_id": "1"}, {"document_id": "2"},
             ]), \
             patch.object(eurlex_mod, "extract_doc", return_value={"title": "T"}), \
             patch.object(eurlex_mod, "process_doc", return_value={"title": "T"}), \
             patch.object(eurlex_mod, "validate_and_load",
                          side_effect=["pass", "quarantine"]):

            mock_pg.connect.return_value = MagicMock()
            result = eurlex_mod.ingest_eurlex_flow.fn(since=SINCE)
            assert result["loaded"] == 1
            assert result["quarantined"] == 1

    def test_skips_duplicate(self):
        mock_llm = MagicMock()

        with patch.object(eurlex_mod, "LLMClient", return_value=mock_llm), \
             patch.object(eurlex_mod, "psycopg2") as mock_pg, \
             patch.object(eurlex_mod, "fetch_eurlex", return_value=[
                 {"document_id": "1"},
             ]), \
             patch.object(eurlex_mod, "extract_doc", return_value={"title": "T"}), \
             patch.object(eurlex_mod, "process_doc", return_value={
                 "title": "T", "_skip": True,
             }), \
             patch.object(eurlex_mod, "validate_and_load") as mock_val:

            mock_pg.connect.return_value = MagicMock()
            result = eurlex_mod.ingest_eurlex_flow.fn(since=SINCE)
            mock_val.assert_not_called()
            assert result["loaded"] == 0

    def test_closes_connection(self):
        mock_conn = MagicMock()
        with patch.object(eurlex_mod, "LLMClient"), \
             patch.object(eurlex_mod, "psycopg2") as mock_pg, \
             patch.object(eurlex_mod, "fetch_eurlex", return_value=[]):
            mock_pg.connect.return_value = mock_conn
            eurlex_mod.ingest_eurlex_flow.fn(since=SINCE)
            mock_conn.close.assert_called_once()


# ===========================================================================
# Standalone US flow
# ===========================================================================

class TestIngestUsFlow:

    def test_flow_processes_documents(self):
        mock_llm = MagicMock()
        raw_docs = [
            {"document_id": "2026-001", "source_name": "Federal Register",
             "full_text": "text"},
        ]

        with patch.object(us_mod, "LLMClient", return_value=mock_llm), \
             patch.object(us_mod, "psycopg2") as mock_pg, \
             patch.object(us_mod, "fetch_us", return_value=raw_docs), \
             patch.object(us_mod, "extract_doc", return_value={
                 "title": "T", "country": "US", "document_type": "regulation",
             }), \
             patch.object(us_mod, "process_doc", return_value={
                 "title": "T", "country": "US", "document_type": "regulation",
             }), \
             patch.object(us_mod, "validate_and_load", return_value="pass"):

            mock_pg.connect.return_value = MagicMock()
            result = us_mod.ingest_us_flow.fn(since=SINCE)

            assert result["source"] == "Federal Register"
            assert result["ingested"] == 1
            assert result["loaded"] == 1

    def test_handles_processing_failure(self):
        mock_llm = MagicMock()

        with patch.object(us_mod, "LLMClient", return_value=mock_llm), \
             patch.object(us_mod, "psycopg2") as mock_pg, \
             patch.object(us_mod, "fetch_us", return_value=[
                 {"document_id": "1"}, {"document_id": "2"},
             ]), \
             patch.object(us_mod, "extract_doc",
                          side_effect=[{"title": "T"}, Exception("fail")]), \
             patch.object(us_mod, "process_doc", return_value={"title": "T"}), \
             patch.object(us_mod, "validate_and_load", return_value="pass"):

            mock_pg.connect.return_value = MagicMock()
            result = us_mod.ingest_us_flow.fn(since=SINCE)
            assert result["loaded"] == 1
            assert result["failed"] == 1

    def test_closes_connection(self):
        mock_conn = MagicMock()
        with patch.object(us_mod, "LLMClient"), \
             patch.object(us_mod, "psycopg2") as mock_pg, \
             patch.object(us_mod, "fetch_us", return_value=[]):
            mock_pg.connect.return_value = mock_conn
            us_mod.ingest_us_flow.fn(since=SINCE)
            mock_conn.close.assert_called_once()
