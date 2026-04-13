"""Tests for pipeline/process.py — clean, enrich, process, process_batch."""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from pipeline.process import (
    clean,
    enrich,
    process,
    process_batch,
    _normalise_date,
    _is_duplicate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**overrides) -> dict:
    """Build a minimal regulation record."""
    base = {
        "source_name": "EUR-Lex",
        "document_id": "32006L0042",
        "title": "Machinery Directive",
        "document_type": "directive",
        "country": "EU",
        "full_text": "This directive applies to machinery.",
    }
    base.update(overrides)
    return base


def _mock_conn(duplicate=False):
    """Build a mock DB connection for dedup checks."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.return_value = (1,) if duplicate else None
    return conn


def _mock_llm(summary="A summary of the regulation."):
    llm = MagicMock()
    llm.generate_summary.return_value = summary
    return llm


# ===========================================================================
# clean
# ===========================================================================

class TestClean:

    def test_strips_whitespace(self):
        rec = _record(title="  Machinery Directive  ", country="  EU  ")
        result = clean(rec)
        assert result["title"] == "Machinery Directive"
        assert result["country"] == "EU"

    def test_normalises_iso_date(self):
        rec = _record(effective_date="2009-12-29")
        result = clean(rec)
        assert result["effective_date"] == "2009-12-29"

    def test_normalises_us_date(self):
        rec = _record(effective_date="01/15/2026")
        result = clean(rec)
        assert result["effective_date"] == "2026-01-15"

    def test_normalises_long_date(self):
        rec = _record(effective_date="29 December 2009")
        result = clean(rec)
        assert result["effective_date"] == "2009-12-29"

    def test_normalises_iso_datetime(self):
        rec = _record(effective_date="2026-01-15T10:00:00Z")
        result = clean(rec)
        assert result["effective_date"] == "2026-01-15"

    def test_null_date_stays_none(self):
        rec = _record(effective_date=None)
        result = clean(rec)
        assert result["effective_date"] is None

    def test_missing_date_field_ok(self):
        rec = _record()
        result = clean(rec)
        assert "effective_date" not in result or result.get("effective_date") is None

    def test_unparseable_date_becomes_none(self):
        rec = _record(effective_date="not-a-date")
        result = clean(rec)
        assert result["effective_date"] is None

    def test_computes_content_hash(self):
        rec = _record(full_text="test content")
        result = clean(rec)
        expected = hashlib.sha256(b"test content").hexdigest()
        assert result["content_hash"] == expected

    def test_empty_full_text_hash_is_none(self):
        rec = _record(full_text="")
        result = clean(rec)
        assert result["content_hash"] is None

    def test_dedup_marks_skip(self):
        conn = _mock_conn(duplicate=True)
        rec = _record()
        result = clean(rec, db_conn=conn)
        assert result["_skip"] is True

    def test_no_dedup_when_new(self):
        conn = _mock_conn(duplicate=False)
        rec = _record()
        result = clean(rec, db_conn=conn)
        assert "_skip" not in result

    def test_no_dedup_when_no_conn(self):
        rec = _record()
        result = clean(rec, db_conn=None)
        assert "_skip" not in result

    def test_dedup_uses_parameterised_query(self):
        conn = _mock_conn(duplicate=False)
        rec = _record()
        clean(rec, db_conn=conn)
        cur = conn.cursor.return_value
        for c in cur.execute.call_args_list:
            sql = c.args[0]
            assert "%s" in sql

    def test_strips_only_string_fields(self):
        """Non-string fields should not be affected."""
        rec = _record(confidence=0.95)
        result = clean(rec)
        assert result["confidence"] == 0.95

    def test_multiple_date_fields(self):
        rec = _record(effective_date="01/15/2026", expiry_date="12/31/2027")
        result = clean(rec)
        assert result["effective_date"] == "2026-01-15"
        assert result["expiry_date"] == "2027-12-31"


# ===========================================================================
# enrich
# ===========================================================================

class TestEnrich:

    def test_adds_summary(self):
        llm = _mock_llm("A directive about machinery safety.")
        rec = _record(full_text="Full text of the machinery directive.")
        result = enrich(rec, llm)
        assert result["summary"] == "A directive about machinery safety."
        llm.generate_summary.assert_called_once_with(
            "Full text of the machinery directive.", max_words=150,
        )

    def test_skips_summary_when_no_full_text(self):
        llm = _mock_llm()
        rec = _record(full_text="")
        result = enrich(rec, llm)
        llm.generate_summary.assert_not_called()

    def test_skips_summary_when_already_present(self):
        llm = _mock_llm()
        rec = _record(summary="Existing summary")
        result = enrich(rec, llm)
        assert result["summary"] == "Existing summary"
        llm.generate_summary.assert_not_called()

    def test_handles_summary_failure(self):
        llm = MagicMock()
        llm.generate_summary.side_effect = Exception("LLM timeout")
        rec = _record(full_text="Some text")
        result = enrich(rec, llm)
        # Should not raise; summary stays absent
        assert result.get("summary") is None or result.get("summary") == ""

    def test_max_words_150(self):
        llm = _mock_llm()
        rec = _record(full_text="Content here")
        enrich(rec, llm)
        call_kwargs = llm.generate_summary.call_args
        assert call_kwargs.kwargs.get("max_words") == 150 or call_kwargs[0][1] == 150


# ===========================================================================
# process
# ===========================================================================

class TestProcess:

    def test_runs_clean_then_enrich(self):
        llm = _mock_llm("Summary text")
        rec = _record(
            title="  Test  ",
            effective_date="01/15/2026",
            full_text="Full text here",
        )
        result = process(rec, llm)
        assert result["title"] == "Test"
        assert result["effective_date"] == "2026-01-15"
        assert result["summary"] == "Summary text"

    def test_skips_enrich_on_duplicate(self):
        conn = _mock_conn(duplicate=True)
        llm = _mock_llm()
        rec = _record()
        result = process(rec, llm, db_conn=conn)
        assert result["_skip"] is True
        llm.generate_summary.assert_not_called()

    def test_enrich_runs_when_not_duplicate(self):
        conn = _mock_conn(duplicate=False)
        llm = _mock_llm("Summary")
        rec = _record(full_text="Some document text")
        result = process(rec, llm, db_conn=conn)
        assert "_skip" not in result
        assert result["summary"] == "Summary"

    def test_process_without_db_conn(self):
        llm = _mock_llm("Summary")
        rec = _record(full_text="text")
        result = process(rec, llm)
        assert result["summary"] == "Summary"


# ===========================================================================
# process_batch
# ===========================================================================

class TestProcessBatch:

    def test_processes_all_records(self):
        llm = _mock_llm("Summary")
        records = [
            _record(document_id="doc1", full_text="text1"),
            _record(document_id="doc2", full_text="text2"),
        ]
        processed, failed = process_batch(records, llm)
        assert len(processed) == 2
        assert len(failed) == 0

    def test_isolates_failures(self):
        llm = MagicMock()
        # First call succeeds, second raises
        llm.generate_summary.side_effect = ["OK", Exception("LLM down")]
        records = [
            _record(document_id="doc1", full_text="text1", summary=""),
            _record(document_id="doc2", full_text="text2", summary=""),
        ]
        # The second record's enrich will fail but the exception in enrich
        # is caught internally, so both should succeed
        processed, failed = process_batch(records, llm)
        # Both should be in processed since enrich catches its own errors
        assert len(processed) == 2
        assert len(failed) == 0

    def test_captures_catastrophic_failure(self):
        """If clean itself raises (not just enrich), the record goes to failed."""
        llm = _mock_llm()
        records = [
            _record(document_id="doc1"),
            "not a dict",  # Will cause clean to fail
        ]
        processed, failed = process_batch(records, llm)
        assert len(processed) == 1
        assert len(failed) == 1
        assert failed[0]["error"]

    def test_failed_record_has_original_and_error(self):
        llm = _mock_llm()
        bad = "not a dict"
        processed, failed = process_batch([bad], llm)
        assert len(failed) == 1
        assert failed[0]["record"] == bad
        assert isinstance(failed[0]["error"], str)

    def test_empty_batch(self):
        llm = _mock_llm()
        processed, failed = process_batch([], llm)
        assert processed == []
        assert failed == []

    def test_skipped_duplicates_still_in_processed(self):
        conn = _mock_conn(duplicate=True)
        llm = _mock_llm()
        records = [_record(document_id="dup1")]
        processed, failed = process_batch(records, llm, db_conn=conn)
        assert len(processed) == 1
        assert processed[0]["_skip"] is True
        assert len(failed) == 0

    def test_batch_logs_summary(self):
        llm = _mock_llm()
        records = [_record(), _record(document_id="doc2")]
        with patch("pipeline.process.logger") as mock_logger:
            process_batch(records, llm)
            mock_logger.info.assert_any_call(
                "Batch complete: %d processed, %d failed", 2, 0,
            )

    def test_batch_with_db_conn(self):
        conn = _mock_conn(duplicate=False)
        llm = _mock_llm("Summary")
        records = [_record(full_text="text")]
        processed, failed = process_batch(records, llm, db_conn=conn)
        assert len(processed) == 1
        assert processed[0]["summary"] == "Summary"


# ===========================================================================
# _normalise_date
# ===========================================================================

class TestNormaliseDate:

    def test_iso_format(self):
        assert _normalise_date("2026-01-15") == "2026-01-15"

    def test_us_format(self):
        assert _normalise_date("01/15/2026") == "2026-01-15"

    def test_long_format(self):
        assert _normalise_date("29 December 2009") == "2009-12-29"

    def test_iso_datetime(self):
        assert _normalise_date("2026-01-15T10:00:00Z") == "2026-01-15"

    def test_iso_datetime_no_tz(self):
        assert _normalise_date("2026-01-15T10:00:00") == "2026-01-15"

    def test_none_returns_none(self):
        assert _normalise_date(None) is None

    def test_empty_returns_none(self):
        assert _normalise_date("") is None

    def test_whitespace_only_returns_none(self):
        assert _normalise_date("   ") is None

    def test_garbage_returns_none(self):
        assert _normalise_date("not-a-date-at-all") is None

    def test_strips_whitespace(self):
        assert _normalise_date("  2026-01-15  ") == "2026-01-15"


# ===========================================================================
# _is_duplicate
# ===========================================================================

class TestIsDuplicate:

    def test_returns_true_when_found(self):
        conn = _mock_conn(duplicate=True)
        assert _is_duplicate("EUR-Lex", "32006L0042", "abc123", conn) is True

    def test_returns_false_when_not_found(self):
        conn = _mock_conn(duplicate=False)
        assert _is_duplicate("EUR-Lex", "32006L0042", "abc123", conn) is False

    def test_uses_parameterised_query(self):
        conn = _mock_conn(duplicate=False)
        _is_duplicate("EUR-Lex", "32006L0042", "abc123", conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "%s" in sql

    def test_null_hash_checks_name_and_id_only(self):
        conn = _mock_conn(duplicate=False)
        _is_duplicate("EUR-Lex", "32006L0042", None, conn)
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "content_hash" not in sql

    def test_closes_cursor(self):
        conn = _mock_conn(duplicate=False)
        _is_duplicate("FR", "123", "hash", conn)
        conn.cursor.return_value.close.assert_called_once()

    def test_closes_cursor_on_error(self):
        conn = _mock_conn(duplicate=False)
        conn.cursor.return_value.execute.side_effect = Exception("db error")
        with pytest.raises(Exception):
            _is_duplicate("FR", "123", "hash", conn)
        conn.cursor.return_value.close.assert_called_once()
