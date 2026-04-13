"""Tests for pipeline/review_cli.py.

All DB interactions are mocked. Rich console output is captured via StringIO.
"""

import io
from unittest.mock import MagicMock, patch, call

import pytest
from rich.console import Console

from pipeline import review_cli
from pipeline.review_cli import (
    pipeline_list,
    pipeline_show,
    pipeline_resolve,
    pipeline_dismiss,
    kb_list,
    kb_show,
    kb_accept,
    kb_reject,
    kb_edit,
    kb_stats,
    build_parser,
    main,
    _count_fields,
    _update_jsonb_field,
    _update_profile_field,
    _update_related_field,
    KB_TRACKED_TABLES,
    KB_PROFILE_JSONB_FIELDS,
    PROFILE_UPDATABLE_FIELDS,
)


@pytest.fixture()
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


@pytest.fixture(autouse=True)
def capture_console(monkeypatch):
    """Replace the module-level console with one that writes to a buffer."""
    buf = io.StringIO()
    test_console = Console(file=buf, no_color=True, width=120)
    monkeypatch.setattr(review_cli, "console", test_console)
    return buf


# ==================================================================
# Parser tests
# ==================================================================

class TestParser:
    def test_pipeline_list(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", "list"])
        assert args.group == "pipeline"
        assert args.command == "list"

    def test_pipeline_show(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", "show", "42"])
        assert args.id == 42

    def test_pipeline_resolve_with_note(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", "resolve", "5", "--note", "looks good"])
        assert args.id == 5
        assert args.note == "looks good"

    def test_pipeline_dismiss(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", "dismiss", "7"])
        assert args.id == 7

    def test_kb_list_with_filters(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "list", "--country", "DE", "--issue", "low_confidence"])
        assert args.country == "DE"
        assert args.issue == "low_confidence"

    def test_kb_show(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "show", "10"])
        assert args.id == 10

    def test_kb_accept(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "accept", "3"])
        assert args.id == 3

    def test_kb_reject_with_note(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "reject", "4", "--note", "wrong"])
        assert args.id == 4
        assert args.note == "wrong"

    def test_kb_edit(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "edit", "8"])
        assert args.id == 8

    def test_kb_stats_with_country(self):
        parser = build_parser()
        args = parser.parse_args(["kb", "stats", "--country", "US"])
        assert args.country == "US"


# ==================================================================
# Pipeline subcommand tests
# ==================================================================

class TestPipelineList:
    def test_list_empty(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        pipeline_list(mock_conn)
        output = capture_console.getvalue()
        assert "No pending pipeline items" in output

    def test_list_with_rows(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = [
            {
                "id": 1,
                "record_type": "regulation",
                "issue_type": "missing_field",
                "issue_detail": "title is empty",
                "created_at": "2024-01-01 00:00:00",
            },
            {
                "id": 2,
                "record_type": "hs_mapping",
                "issue_type": "low_confidence",
                "issue_detail": "confidence=0.42",
                "created_at": "2024-01-02 00:00:00",
            },
        ]

        pipeline_list(mock_conn)
        output = capture_console.getvalue()
        assert "regulation" in output
        assert "missing_field" in output
        assert "2 pending" in output

    def test_list_truncates_long_detail(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        long_detail = "x" * 100
        cur.fetchall.return_value = [
            {
                "id": 1,
                "record_type": "regulation",
                "issue_type": "corrupt_data",
                "issue_detail": long_detail,
                "created_at": "2024-01-01",
            },
        ]

        pipeline_list(mock_conn)
        output = capture_console.getvalue()
        assert "..." in output


class TestPipelineShow:
    def test_show_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 1,
            "record_type": "regulation",
            "record_id": 42,
            "issue_type": "missing_field",
            "issue_detail": "title is empty",
            "status": "pending",
            "created_at": "2024-01-01",
            "source_url": "https://example.com/doc",
        }

        pipeline_show(mock_conn, 1)
        output = capture_console.getvalue()
        assert "Pipeline Item #1" in output
        assert "https://example.com/doc" in output

    def test_show_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        pipeline_show(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output

    def test_show_no_source_url(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 2,
            "record_type": "hs_mapping",
            "record_id": None,
            "issue_type": "new_mapping",
            "issue_detail": "{}",
            "status": "pending",
            "created_at": "2024-01-01",
            "source_url": None,
        }

        pipeline_show(mock_conn, 2)
        output = capture_console.getvalue()
        assert "N/A" in output


class TestPipelineResolve:
    def test_resolve_success(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"id": 1}

        pipeline_resolve(mock_conn, 1)
        output = capture_console.getvalue()
        assert "resolved" in output
        mock_conn.commit.assert_called_once()

    def test_resolve_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        pipeline_resolve(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output


class TestPipelineDismiss:
    def test_dismiss_success(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"id": 1}

        pipeline_dismiss(mock_conn, 1)
        output = capture_console.getvalue()
        assert "dismissed" in output
        mock_conn.commit.assert_called_once()

    def test_dismiss_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        pipeline_dismiss(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output


# ==================================================================
# KB subcommand tests
# ==================================================================

class TestKbList:
    def test_list_empty(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_list(mock_conn)
        output = capture_console.getvalue()
        assert "No pending KB items" in output

    def test_list_with_rows(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = [
            {
                "id": 10,
                "country": "DE",
                "table_name": "kb_memberships",
                "field_name": "is_member",
                "issue_type": "low_confidence",
                "confidence": 0.55,
                "created_at": "2024-03-01",
            },
        ]

        kb_list(mock_conn)
        output = capture_console.getvalue()
        assert "DE" in output
        assert "kb_memberships" in output
        assert "0.55" in output

    def test_list_filter_country(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_list(mock_conn, country="US")
        sql = cur.execute.call_args[0][0]
        assert "cp.iso2 = %s" in sql

    def test_list_filter_issue(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_list(mock_conn, issue="spot_check")
        sql = cur.execute.call_args[0][0]
        assert "vq.issue_type = %s" in sql

    def test_list_filter_all_issue(self, mock_conn, capture_console):
        """issue='all' should not add an issue_type WHERE filter."""
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_list(mock_conn, issue="all")
        sql = cur.execute.call_args[0][0]
        assert "vq.issue_type = %s" not in sql


class TestKbShow:
    def test_show_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 5,
            "country_id": 1,
            "country": "DE",
            "table_name": "kb_memberships",
            "record_id": 10,
            "field_name": "is_member",
            "current_value": "False",
            "proposed_value": "True",
            "issue_type": "low_confidence",
            "confidence": 0.55,
            "source_url": "https://wto.org/members",
            "conflict_source_url": "https://other.org/data",
            "status": "pending",
            "reviewer_note": None,
            "created_at": "2024-03-01",
            "resolved_at": None,
        }

        kb_show(mock_conn, 5)
        output = capture_console.getvalue()
        assert "KB Item #5" in output
        assert "Current Value" in output
        assert "Proposed Value" in output
        assert "https://wto.org/members" in output
        assert "https://other.org/data" in output

    def test_show_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        kb_show(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output


class TestKbAccept:
    def test_accept_related_table(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 5,
            "table_name": "kb_memberships",
            "record_id": 10,
            "field_name": "scope_details",
            "proposed_value": "Full WTO member",
            "status": "pending",
        }

        kb_accept(mock_conn, 5)
        output = capture_console.getvalue()
        assert "accepted" in output
        assert "kb_memberships.scope_details" in output
        mock_conn.commit.assert_called_once()

        # Verify the UPDATE was called with confidence=0.95
        update_calls = [
            c for c in cur.execute.call_args_list
            if "confidence = 0.95" in str(c)
        ]
        assert len(update_calls) == 1

    def test_accept_jsonb_field(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 6,
            "table_name": "kb_country_profiles",
            "record_id": 1,
            "field_name": "national_standards_body",
            "proposed_value": "DIN",
            "status": "pending",
        }

        kb_accept(mock_conn, 6)
        output = capture_console.getvalue()
        assert "accepted" in output

        # Verify jsonb_set was used
        update_calls = [
            c for c in cur.execute.call_args_list
            if "jsonb_set" in str(c)
        ]
        assert len(update_calls) == 1

    def test_accept_profile_plain_field(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 7,
            "table_name": "kb_country_profiles",
            "record_id": 1,
            "field_name": "region",
            "proposed_value": "Western Europe",
            "status": "pending",
        }

        kb_accept(mock_conn, 7)
        output = capture_console.getvalue()
        assert "accepted" in output

    def test_accept_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        kb_accept(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output

    def test_accept_missing_table_name(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 8,
            "table_name": None,
            "record_id": 1,
            "field_name": "foo",
            "proposed_value": "bar",
            "status": "pending",
        }

        kb_accept(mock_conn, 8)
        output = capture_console.getvalue()
        assert "Cannot accept" in output

    def test_accept_disallowed_profile_field(self, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 9,
            "table_name": "kb_country_profiles",
            "record_id": 1,
            "field_name": "iso2",
            "proposed_value": "XX",
            "status": "pending",
        }

        with pytest.raises(ValueError, match="not updatable"):
            kb_accept(mock_conn, 9)

    def test_accept_disallowed_table(self, mock_conn):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 10,
            "table_name": "users",
            "record_id": 1,
            "field_name": "name",
            "proposed_value": "hacked",
            "status": "pending",
        }

        with pytest.raises(ValueError, match="not updatable"):
            kb_accept(mock_conn, 10)


class TestKbReject:
    def test_reject_success(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {"id": 5}

        kb_reject(mock_conn, 5, note="Incorrect data")
        output = capture_console.getvalue()
        assert "rejected" in output
        mock_conn.commit.assert_called_once()

        # Verify note was passed
        params = cur.execute.call_args[0][1]
        assert params[0] == "Incorrect data"

    def test_reject_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        kb_reject(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output


class TestKbEdit:
    def test_edit_related_table(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 5,
            "table_name": "kb_laws",
            "record_id": 3,
            "field_name": "title",
            "current_value": "Old Title",
            "proposed_value": "AI Proposed Title",
            "status": "pending",
        }

        # Mock console.input
        with patch.object(review_cli.console, "input", return_value="Correct Title"):
            kb_edit(mock_conn, 5)

        output = capture_console.getvalue()
        assert "updated with manual value" in output
        assert "confidence=1.0" in output
        mock_conn.commit.assert_called_once()

        # Verify confidence=1.0 in the update
        update_calls = [
            c for c in cur.execute.call_args_list
            if "confidence = 1.0" in str(c)
        ]
        assert len(update_calls) == 1

    def test_edit_empty_input_cancels(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 5,
            "table_name": "kb_laws",
            "record_id": 3,
            "field_name": "title",
            "current_value": "Old",
            "proposed_value": "New",
            "status": "pending",
        }

        with patch.object(review_cli.console, "input", return_value="   "):
            kb_edit(mock_conn, 5)

        output = capture_console.getvalue()
        assert "Cancelled" in output
        mock_conn.commit.assert_not_called()

    def test_edit_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = None

        kb_edit(mock_conn, 999)
        output = capture_console.getvalue()
        assert "not found" in output

    def test_edit_jsonb_field(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 6,
            "table_name": "kb_country_profiles",
            "record_id": 1,
            "field_name": "accreditation_body",
            "current_value": "OldBody",
            "proposed_value": "AIBody",
            "status": "pending",
        }

        with patch.object(review_cli.console, "input", return_value="DAkkS"):
            kb_edit(mock_conn, 6)

        output = capture_console.getvalue()
        assert "updated with manual value" in output

        # Verify jsonb_set was called
        update_calls = [
            c for c in cur.execute.call_args_list
            if "jsonb_set" in str(c)
        ]
        assert len(update_calls) == 1

    def test_edit_disallowed_table(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchone.return_value = {
            "id": 7,
            "table_name": "evil_table",
            "record_id": 1,
            "field_name": "x",
            "current_value": None,
            "proposed_value": None,
            "status": "pending",
        }

        with patch.object(review_cli.console, "input", return_value="val"):
            kb_edit(mock_conn, 7)

        output = capture_console.getvalue()
        assert "not updatable" in output


class TestKbStats:
    def test_stats_single_country(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value

        # First call: get country
        # Then _count_fields: 7 tables * 2 queries each = 14 calls
        # Then 4 JSONB field queries
        # Then pending count
        country_row = {"id": 1, "iso2": "DE", "country_name": "Germany"}

        fetchall_returns = [[country_row]]
        fetchone_returns = []

        # For each tracked table: row count + stale count
        for _ in KB_TRACKED_TABLES:
            fetchone_returns.append({"cnt": 3})  # row count
            fetchone_returns.append({"cnt": 1})  # stale count

        # For each JSONB field
        for field in KB_PROFILE_JSONB_FIELDS:
            fetchone_returns.append({field: {"value": "something"}})

        # Pending count
        fetchone_returns.append({"cnt": 2})

        cur.fetchall.side_effect = fetchall_returns
        cur.fetchone.side_effect = fetchone_returns

        kb_stats(mock_conn, country="DE")
        output = capture_console.getvalue()
        assert "DE" in output
        assert "Germany" in output
        assert "Completeness Report" in output

    def test_stats_no_countries(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_stats(mock_conn)
        output = capture_console.getvalue()
        assert "No countries" in output

    def test_stats_country_not_found(self, mock_conn, capture_console):
        cur = mock_conn.cursor.return_value
        cur.fetchall.return_value = []

        kb_stats(mock_conn, country="ZZ")
        output = capture_console.getvalue()
        assert "not found" in output


class TestCountFields:
    def test_count_fields_basic(self, mock_conn):
        cur = mock_conn.cursor.return_value

        fetchone_returns = []
        # 7 tables * 2 queries
        for _ in KB_TRACKED_TABLES:
            fetchone_returns.append({"cnt": 2})  # rows
            fetchone_returns.append({"cnt": 0})  # stale

        # 4 JSONB fields — all populated
        for field in KB_PROFILE_JSONB_FIELDS:
            fetchone_returns.append({field: {"value": "x", "last_verified_at": "2024-01-01"}})

        cur.fetchone.side_effect = fetchone_returns

        total, populated, stale = _count_fields(cur, 1)
        # 7 tables * 2 rows + 4 JSONB = 18 total
        assert total == 7 * 2 + 4
        assert populated == 7 * 2 + 4
        assert stale == 0

    def test_count_fields_with_missing_jsonb(self, mock_conn):
        cur = mock_conn.cursor.return_value

        fetchone_returns = []
        for _ in KB_TRACKED_TABLES:
            fetchone_returns.append({"cnt": 0})
            fetchone_returns.append({"cnt": 0})

        # 2 populated, 2 None
        for i, field in enumerate(KB_PROFILE_JSONB_FIELDS):
            if i < 2:
                fetchone_returns.append({field: {"value": "x"}})
            else:
                fetchone_returns.append({field: None})

        cur.fetchone.side_effect = fetchone_returns

        total, populated, stale = _count_fields(cur, 1)
        assert total == 4  # only JSONB fields since tables have 0 rows
        assert populated == 2
        # The 2 populated ones have no last_verified_at -> stale
        assert stale == 2


# ==================================================================
# Update helper tests
# ==================================================================

class TestUpdateHelpers:
    def test_update_profile_field_allowed(self):
        cur = MagicMock()
        _update_profile_field(cur, 1, "region", "Europe")
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "region = %s" in sql

    def test_update_profile_field_disallowed(self):
        cur = MagicMock()
        with pytest.raises(ValueError):
            _update_profile_field(cur, 1, "iso2", "XX")

    def test_update_related_field_allowed(self):
        cur = MagicMock()
        _update_related_field(cur, "kb_memberships", 1, "scope_details", "Full")
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "confidence = 0.95" in sql
        assert "last_verified_at = NOW()" in sql

    def test_update_related_field_disallowed_table(self):
        cur = MagicMock()
        with pytest.raises(ValueError):
            _update_related_field(cur, "evil_table", 1, "x", "y")

    def test_update_jsonb_field(self):
        cur = MagicMock()
        _update_jsonb_field(cur, 1, "national_standards_body", "DIN")
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "jsonb_set" in sql


# ==================================================================
# main() dispatch tests
# ==================================================================

class TestMain:
    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.pipeline_list")
    def test_main_pipeline_list(self, mock_pl, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["pipeline", "list"])
        mock_pl.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.pipeline_show")
    def test_main_pipeline_show(self, mock_ps, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["pipeline", "show", "1"])
        mock_ps.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.pipeline_resolve")
    def test_main_pipeline_resolve(self, mock_pr, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["pipeline", "resolve", "1", "--note", "ok"])
        mock_pr.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.pipeline_dismiss")
    def test_main_pipeline_dismiss(self, mock_pd, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["pipeline", "dismiss", "3"])
        mock_pd.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_list")
    def test_main_kb_list(self, mock_kl, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "list", "--country", "DE"])
        mock_kl.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_show")
    def test_main_kb_show(self, mock_ks, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "show", "5"])
        mock_ks.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_accept")
    def test_main_kb_accept(self, mock_ka, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "accept", "5"])
        mock_ka.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_reject")
    def test_main_kb_reject(self, mock_kr, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "reject", "5", "--note", "nope"])
        mock_kr.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_edit")
    def test_main_kb_edit(self, mock_ke, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "edit", "5"])
        mock_ke.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    @patch("pipeline.review_cli.kb_stats")
    def test_main_kb_stats(self, mock_ks, mock_conn):
        mock_conn.return_value = MagicMock()
        main(["kb", "stats"])
        mock_ks.assert_called_once()

    @patch("pipeline.review_cli.get_connection")
    def test_main_closes_connection(self, mock_get_conn):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchall.return_value = []
        mock_get_conn.return_value = conn

        main(["pipeline", "list"])
        conn.close.assert_called_once()
