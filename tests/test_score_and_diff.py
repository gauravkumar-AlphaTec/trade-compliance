"""Tests for kb/score_confidence.py and kb/diff_profile.py."""

from unittest.mock import MagicMock, call

import pytest

from kb.score_confidence import (
    calculate_confidence,
    route_field,
    queue_item,
    TIER_ADJUSTMENTS,
    FREE_TEXT_FIELDS,
)
from kb.diff_profile import (
    diff_scalar,
    diff_text,
    diff_list,
    diff_country_profile,
    TEXT_SIMILARITY_THRESHOLD,
)


# ═══════════════════════════════════════════════════════════════════
# score_confidence.py
# ═══════════════════════════════════════════════════════════════════

class TestCalculateConfidence:

    def test_tier1_boost(self):
        result = calculate_confidence(0.80, source_tier=1)
        assert result == pytest.approx(0.95)

    def test_tier2_boost(self):
        result = calculate_confidence(0.80, source_tier=2)
        assert result == pytest.approx(0.85)

    def test_tier3_no_change(self):
        result = calculate_confidence(0.80, source_tier=3)
        assert result == pytest.approx(0.80)

    def test_tier4_penalty(self):
        result = calculate_confidence(0.80, source_tier=4)
        assert result == pytest.approx(0.70)

    def test_cross_validated_bonus(self):
        result = calculate_confidence(0.80, source_tier=3, cross_validated=True)
        assert result == pytest.approx(0.90)

    def test_conflict_penalty(self):
        result = calculate_confidence(0.80, source_tier=3, conflict_detected=True)
        assert result == pytest.approx(0.60)

    def test_both_modifiers(self):
        # 0.80 + 0.00 (tier3) + 0.10 (cross) - 0.20 (conflict) = 0.70
        result = calculate_confidence(
            0.80, source_tier=3, cross_validated=True, conflict_detected=True
        )
        assert result == pytest.approx(0.70)

    def test_clamps_at_1(self):
        result = calculate_confidence(0.95, source_tier=1, cross_validated=True)
        assert result == 1.0

    def test_clamps_at_0(self):
        result = calculate_confidence(0.10, source_tier=4, conflict_detected=True)
        assert result == 0.0

    def test_unknown_tier_no_adjustment(self):
        result = calculate_confidence(0.80, source_tier=99)
        assert result == pytest.approx(0.80)


class TestRouteField:

    def test_auto_accept_high_confidence(self):
        assert route_field("accreditation_body", "DAkkS", 0.95) == "auto_accept"

    def test_auto_accept_boundary(self):
        assert route_field("accreditation_body", "DAkkS", 0.90) == "auto_accept"

    def test_spot_check_mid_confidence(self):
        assert route_field("accreditation_body", "DAkkS", 0.85) == "spot_check"

    def test_spot_check_lower_boundary(self):
        assert route_field("accreditation_body", "DAkkS", 0.70) == "spot_check"

    def test_hold_low_confidence(self):
        assert route_field("accreditation_body", "DAkkS", 0.69) == "hold"

    def test_hold_zero(self):
        assert route_field("accreditation_body", "DAkkS", 0.0) == "hold"

    def test_free_text_always_spot_check(self):
        for field in ("local_challenges", "recent_reforms", "general_notes"):
            assert route_field(field, "some text", 0.99) == "spot_check"
            assert route_field(field, "some text", 0.50) == "spot_check"

    def test_non_free_text_field_uses_thresholds(self):
        assert route_field("iso2", "DE", 0.95) == "auto_accept"

    def test_generic_product_categories_forced_hold(self):
        assert route_field("product_categories", ["machinery"], 0.95) == "hold"

    def test_generic_categories_case_insensitive(self):
        assert route_field("product_categories", ["Equipment"], 0.95) == "hold"

    def test_generic_categories_whitespace_trimmed(self):
        assert route_field("product_categories", ["  devices  "], 0.90) == "hold"

    def test_multiple_generic_categories_hold(self):
        assert route_field("product_categories", ["goods", "articles"], 0.95) == "hold"

    def test_specific_categories_use_normal_thresholds(self):
        assert route_field("product_categories", ["centrifugal pumps"], 0.95) == "auto_accept"

    def test_mixed_categories_use_normal_thresholds(self):
        assert route_field("product_categories", ["machinery", "centrifugal pumps"], 0.95) == "auto_accept"

    def test_empty_categories_use_normal_thresholds(self):
        assert route_field("product_categories", [], 0.95) == "auto_accept"

    def test_all_generic_terms_trigger_hold(self):
        for term in [
            "pressure equipment", "assemblies", "machinery", "equipment",
            "products", "goods", "articles", "devices",
        ]:
            assert route_field("product_categories", [term], 0.99) == "hold", f"Failed for: {term}"

    def test_other_field_with_list_not_affected(self):
        assert route_field("tags", ["machinery"], 0.95) == "auto_accept"


class TestQueueItem:

    def test_inserts_and_returns_id(self):
        conn = MagicMock()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = (42,)

        result = queue_item(
            country_id=1,
            table_name="kb_memberships",
            record_id=10,
            field_name="is_member",
            current_value="true",
            proposed_value="false",
            confidence=0.65,
            issue_type="low_confidence",
            source_url="https://example.com",
            db_conn=conn,
        )

        assert result == 42
        cur.execute.assert_called_once()
        conn.commit.assert_called_once()
        cur.close.assert_called_once()

    def test_uses_parameterised_query(self):
        conn = MagicMock()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = (1,)

        queue_item(1, "t", None, None, None, None, 0.5, "missing", None, conn)

        sql = cur.execute.call_args.args[0]
        # Must use %s placeholders, never f-strings
        assert "%s" in sql
        assert "f'" not in sql
        params = cur.execute.call_args.args[1]
        assert len(params) == 9

    def test_rollback_on_error(self):
        conn = MagicMock()
        cur = conn.cursor.return_value
        cur.execute.side_effect = Exception("db error")

        with pytest.raises(Exception, match="db error"):
            queue_item(1, "t", None, None, None, None, 0.5, "missing", None, conn)

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


# ═══════════════════════════════════════════════════════════════════
# diff_profile.py
# ═══════════════════════════════════════════════════════════════════

class TestDiffScalar:

    def test_both_none(self):
        assert diff_scalar(None, None) is False

    def test_one_none(self):
        assert diff_scalar(None, "value") is True
        assert diff_scalar("value", None) is True

    def test_equal_strings(self):
        assert diff_scalar("hello", "hello") is False

    def test_whitespace_only_difference(self):
        assert diff_scalar("hello  world", "hello world") is False

    def test_leading_trailing_whitespace(self):
        assert diff_scalar("  hello  ", "hello") is False

    def test_different_strings(self):
        assert diff_scalar("hello", "world") is True

    def test_equal_numbers(self):
        assert diff_scalar(42, 42) is False

    def test_different_numbers(self):
        assert diff_scalar(42, 43) is True

    def test_equal_booleans(self):
        assert diff_scalar(True, True) is False

    def test_different_booleans(self):
        assert diff_scalar(True, False) is True


class TestDiffText:

    def test_identical_texts(self):
        assert diff_text("same text", "same text") is False

    def test_very_different_texts(self):
        assert diff_text("completely different", "nothing alike here at all") is True

    def test_minor_edit_below_threshold(self):
        base = "The quick brown fox jumps over the lazy dog"
        minor = "The quick brown fox leaps over the lazy dog"
        assert diff_text(base, minor) is False

    def test_major_edit_above_threshold(self):
        base = "The quick brown fox jumps over the lazy dog"
        major = "A slow red cat sits under the energetic puppy"
        assert diff_text(base, major) is True

    def test_none_existing(self):
        assert diff_text(None, "new text") is True

    def test_none_extracted(self):
        assert diff_text("old text", None) is True

    def test_both_none(self):
        assert diff_text(None, None) is False


class TestDiffList:

    def test_all_new_when_existing_empty(self):
        extracted = [{"code": "A"}, {"code": "B"}]
        result = diff_list([], extracted, "code")
        assert len(result) == 2

    def test_all_new_when_existing_none(self):
        extracted = [{"code": "A"}]
        result = diff_list(None, extracted, "code")
        assert len(result) == 1

    def test_empty_when_extracted_empty(self):
        assert diff_list([{"code": "A"}], [], "code") == []
        assert diff_list([{"code": "A"}], None, "code") == []

    def test_detects_new_item(self):
        existing = [{"code": "A", "name": "Alpha"}]
        extracted = [
            {"code": "A", "name": "Alpha"},
            {"code": "B", "name": "Beta"},
        ]
        result = diff_list(existing, extracted, "code")
        assert len(result) == 1
        assert result[0]["code"] == "B"

    def test_detects_changed_item(self):
        existing = [{"code": "A", "name": "Alpha"}]
        extracted = [{"code": "A", "name": "Alpha Updated"}]
        result = diff_list(existing, extracted, "code")
        assert len(result) == 1
        assert result[0]["name"] == "Alpha Updated"

    def test_unchanged_excluded(self):
        existing = [{"code": "A", "name": "Alpha"}]
        extracted = [{"code": "A", "name": "Alpha"}]
        result = diff_list(existing, extracted, "code")
        assert result == []

    def test_metadata_ignored_in_comparison(self):
        existing = [{"code": "A", "name": "Alpha", "confidence": 0.9, "last_verified_at": "2025-01-01"}]
        extracted = [{"code": "A", "name": "Alpha", "confidence": 0.8, "last_verified_at": "2026-01-01"}]
        result = diff_list(existing, extracted, "code")
        assert result == []

    def test_item_without_key_always_included(self):
        existing = [{"code": "A"}]
        extracted = [{"name": "no key field"}]
        result = diff_list(existing, extracted, "code")
        assert len(result) == 1


class TestDiffCountryProfile:

    def _make_conn(self, profile_row, list_queries=None):
        """Build a mock db_conn that returns profile_row for the first query
        and optionally serves list table queries."""
        conn = MagicMock()
        cur = conn.cursor.return_value

        call_count = [0]
        list_queries = list_queries or {}

        def mock_execute(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # Profile query
                cur.fetchone.return_value = profile_row
                cur.fetchall.return_value = []
                cur.description = []
            else:
                # List table query
                for table_name, (cols, rows) in list_queries.items():
                    if table_name in sql:
                        cur.description = [(c,) for c in cols]
                        cur.fetchall.return_value = rows
                        return
                cur.description = [("id",)]
                cur.fetchall.return_value = []

        cur.execute = MagicMock(side_effect=mock_execute)
        return conn

    def test_new_profile_all_changed(self):
        conn = self._make_conn(profile_row=None)
        extracted = {"local_challenges": "Some challenges", "iso2": "DE"}
        result = diff_country_profile(1, extracted, conn)
        assert len(result["changed"]) == 2
        assert all(c["change_type"] == "new" for c in result["changed"])
        assert result["unchanged_count"] == 0

    def test_unchanged_scalar(self):
        # Profile row matches the 11 fields queried
        profile_row = (
            None, None, None, None,  # QIB fields
            "Same challenges",       # local_challenges
            None, None,              # recent_reforms, general_notes
            None, None,              # translation_*
            None, None,              # ca_system_structure, accreditation_mandatory
        )
        conn = self._make_conn(profile_row=profile_row)
        extracted = {"local_challenges": "Same challenges"}
        result = diff_country_profile(1, extracted, conn)
        assert len(result["changed"]) == 0
        assert result["unchanged_count"] == 1

    def test_changed_scalar_detected(self):
        profile_row = (
            None, None, None, None,
            "Old challenges",
            None, None,
            None, None,
            None, None,
        )
        conn = self._make_conn(profile_row=profile_row)
        extracted = {"local_challenges": "Completely new and different challenges text"}
        result = diff_country_profile(1, extracted, conn)
        assert len(result["changed"]) == 1
        assert result["changed"][0]["field"] == "local_challenges"
        assert result["changed"][0]["change_type"] == "updated"

    def test_list_diff_detects_new_items(self):
        profile_row = (None,) * 11
        conn = self._make_conn(
            profile_row=profile_row,
            list_queries={
                "kb_memberships": (
                    ["id", "country_id", "org_code", "is_member"],
                    [(1, 1, "WTO", True)],
                ),
            },
        )
        extracted = {
            "memberships": [
                {"org_code": "WTO", "is_member": True},
                {"org_code": "ISO", "is_member": True},
            ],
        }
        result = diff_country_profile(1, extracted, conn)
        new_items = [c for c in result["changed"] if c["table"] == "kb_memberships"]
        assert len(new_items) == 1
        assert new_items[0]["new_value"]["org_code"] == "ISO"
