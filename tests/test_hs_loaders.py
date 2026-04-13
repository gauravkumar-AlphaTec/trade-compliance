"""Tests for hs_library/load_wco.py and hs_library/load_eu_cn.py."""

from unittest.mock import MagicMock, patch

import pytest

from hs_library.load_wco import (
    fetch_csv,
    parse_csv,
    insert_rows,
    load_wco_codes,
    get_csv_url,
    DEFAULT_WCO_CSV_URL,
)
from hs_library.load_eu_cn import (
    fetch_chapter_ids,
    fetch_chapter_detail,
    fetch_heading_detail,
    extract_headings,
    extract_cn8_codes,
    insert_codes,
    load_eu_cn_codes,
    get_base_url,
    DEFAULT_TARIFF_BASE,
)


SAMPLE_WCO_CSV = """section,hscode,description,parent,level
I,01,Animals; live,TOTAL,2
I,0101,"Horses, asses, mules and hinnies; live",01,4
I,010121,"Horses; live, pure-bred breeding animals",0101,6
I,010129,"Horses; live, other than pure-bred breeding",0101,6
"""


# ==================================================================
# WCO loader
# ==================================================================

class TestWcoFetch:
    @patch("hs_library.load_wco.httpx.get")
    def test_fetch_csv_default_url(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_WCO_CSV
        mock_get.return_value = mock_resp

        text = fetch_csv()
        assert "0101" in text
        mock_get.assert_called_once()
        assert mock_get.call_args[0][0] == DEFAULT_WCO_CSV_URL

    @patch("hs_library.load_wco.httpx.get")
    def test_fetch_csv_custom_url(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "section,hscode,description,parent,level\n"
        mock_get.return_value = mock_resp

        fetch_csv("https://example.com/hs.csv")
        assert mock_get.call_args[0][0] == "https://example.com/hs.csv"

    def test_get_csv_url_default(self, monkeypatch):
        monkeypatch.delenv("WCO_HS_CSV_URL", raising=False)
        assert get_csv_url() == DEFAULT_WCO_CSV_URL

    def test_get_csv_url_env_override(self, monkeypatch):
        monkeypatch.setenv("WCO_HS_CSV_URL", "https://other.example/hs.csv")
        assert get_csv_url() == "https://other.example/hs.csv"


class TestWcoParse:
    def test_parse_csv_basic(self):
        rows = parse_csv(SAMPLE_WCO_CSV)
        assert len(rows) == 4
        # Level 6 codes
        level_6 = [r for r in rows if r["code_type"] == "WCO_6"]
        assert len(level_6) == 2
        codes = {r["code"] for r in level_6}
        assert "010121" in codes
        assert "010129" in codes

    def test_parse_csv_chapter_level(self):
        rows = parse_csv(SAMPLE_WCO_CSV)
        chapter = next(r for r in rows if r["code"] == "01")
        assert chapter["code_type"] == "WCO_2"
        assert chapter["description"] == "Animals; live"

    def test_parse_csv_parent_link(self):
        rows = parse_csv(SAMPLE_WCO_CSV)
        sub = next(r for r in rows if r["code"] == "010121")
        assert sub["parent_code"] == "0101"

    def test_parse_csv_country_scope_wco(self):
        rows = parse_csv(SAMPLE_WCO_CSV)
        for r in rows:
            assert r["country_scope"] == "WCO"

    def test_parse_csv_skip_empty(self):
        csv_text = "section,hscode,description,parent,level\nI,,,,\n"
        rows = parse_csv(csv_text)
        assert rows == []

    def test_parse_csv_skip_invalid_level(self):
        csv_text = "section,hscode,description,parent,level\nI,01,Animals,TOTAL,abc\n"
        rows = parse_csv(csv_text)
        assert rows == []

    def test_parse_csv_strips_whitespace(self):
        csv_text = (
            "section,hscode,description,parent,level\n"
            "I,  010121  ,  Horses  ,0101,6\n"
        )
        rows = parse_csv(csv_text)
        assert rows[0]["code"] == "010121"
        assert rows[0]["description"] == "Horses"


class TestWcoInsert:
    def test_insert_rows_calls_db(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.rowcount = 1

        rows = [{
            "code": "010121",
            "code_type": "WCO_6",
            "description": "Horses",
            "parent_code": "0101",
            "country_scope": "WCO",
        }]

        count = insert_rows(rows, conn)
        assert count == 1
        cur.execute.assert_called_once()
        conn.commit.assert_called_once()

    def test_insert_rows_skips_conflicts(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # Simulate ON CONFLICT — rowcount stays at 0
        cur.rowcount = 0

        rows = [{"code": "010121", "code_type": "WCO_6",
                 "description": "x", "parent_code": None, "country_scope": "WCO"}]
        count = insert_rows(rows, conn)
        assert count == 0

    def test_insert_rows_rolls_back_on_error(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.execute.side_effect = Exception("boom")

        rows = [{"code": "x", "code_type": "WCO_6", "description": "x",
                 "parent_code": None, "country_scope": "WCO"}]
        with pytest.raises(Exception, match="boom"):
            insert_rows(rows, conn)
        conn.rollback.assert_called_once()


class TestWcoLoadEnd2End:
    @patch("hs_library.load_wco.httpx.get")
    def test_load_wco_codes(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_WCO_CSV
        mock_get.return_value = mock_resp

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.rowcount = 1

        result = load_wco_codes(conn)
        assert result["fetched"] == 4
        assert result["inserted"] == 4


# ==================================================================
# EU CN loader
# ==================================================================

CHAPTER_PAYLOAD = {
    "data": {"attributes": {"description": "LIVE ANIMALS"}},
    "included": [
        {
            "type": "heading",
            "attributes": {
                "goods_nomenclature_item_id": "0101000000",
                "description": "Live horses",
            },
        },
        {
            "type": "heading",
            "attributes": {
                "goods_nomenclature_item_id": "0102000000",
                "description": "Live bovine animals",
            },
        },
        {
            "type": "section",
            "attributes": {"numeral": "I"},
        },
    ],
}

HEADING_PAYLOAD = {
    "data": {},
    "included": [
        {
            "type": "commodity",
            "attributes": {
                "goods_nomenclature_item_id": "0101210000",
                "producline_suffix": "10",
                "description": "Horses category",
            },
        },
        {
            "type": "commodity",
            "attributes": {
                "goods_nomenclature_item_id": "0101210000",
                "producline_suffix": "80",
                "description": "Pure-bred breeding animals",
            },
        },
        {
            "type": "commodity",
            "attributes": {
                "goods_nomenclature_item_id": "0101290000",
                "producline_suffix": "80",
                "description": "Other horses",
            },
        },
    ],
}


class TestEuCnFetch:
    def test_get_base_url_default(self, monkeypatch):
        monkeypatch.delenv("UK_TARIFF_API_BASE", raising=False)
        assert get_base_url() == DEFAULT_TARIFF_BASE

    def test_get_base_url_env(self, monkeypatch):
        monkeypatch.setenv("UK_TARIFF_API_BASE", "https://custom.example/api")
        assert get_base_url() == "https://custom.example/api"

    def test_fetch_chapter_ids(self):
        client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": [
                {"attributes": {"goods_nomenclature_item_id": "0100000000"}},
                {"attributes": {"goods_nomenclature_item_id": "0200000000"}},
                {"attributes": {"goods_nomenclature_item_id": "9900000000"}},
            ]
        }
        client.get.return_value = resp

        ids = fetch_chapter_ids(client)
        assert ids == ["01", "02", "99"]

    def test_fetch_chapter_ids_skips_invalid(self):
        client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": [
                {"attributes": {"goods_nomenclature_item_id": "0100000000"}},
                {"attributes": {"goods_nomenclature_item_id": ""}},
                {"attributes": {}},
            ]
        }
        client.get.return_value = resp

        ids = fetch_chapter_ids(client)
        assert ids == ["01"]

    def test_fetch_chapter_detail(self):
        client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = CHAPTER_PAYLOAD
        client.get.return_value = resp

        result = fetch_chapter_detail(client, "01")
        assert result == CHAPTER_PAYLOAD
        client.get.assert_called_once_with("/chapters/01", timeout=30)

    def test_fetch_heading_detail(self):
        client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = HEADING_PAYLOAD
        client.get.return_value = resp

        result = fetch_heading_detail(client, "0101")
        assert result == HEADING_PAYLOAD
        client.get.assert_called_once_with("/headings/0101", timeout=30)


class TestEuCnExtract:
    def test_extract_headings(self):
        rows = extract_headings(CHAPTER_PAYLOAD)
        assert len(rows) == 2
        assert rows[0]["code"] == "0101"
        assert rows[0]["description"] == "Live horses"
        assert rows[1]["code"] == "0102"

    def test_extract_headings_skips_non_heading(self):
        payload = {"included": [
            {"type": "footnote", "attributes": {"description": "x"}},
            {"type": "section", "attributes": {"numeral": "I"}},
        ]}
        assert extract_headings(payload) == []

    def test_extract_headings_skips_missing_fields(self):
        payload = {"included": [
            {"type": "heading", "attributes": {"goods_nomenclature_item_id": ""}},
            {"type": "heading", "attributes": {"description": "no id"}},
        ]}
        assert extract_headings(payload) == []

    def test_extract_cn8_codes(self):
        rows = extract_cn8_codes(HEADING_PAYLOAD)
        # Only suffix=80 entries are kept and dedupe by 8-digit code
        assert len(rows) == 2
        codes = {r["code"] for r in rows}
        assert codes == {"01012100", "01012900"}

    def test_extract_cn8_codes_dedupes(self):
        payload = {"included": [
            {
                "type": "commodity",
                "attributes": {
                    "goods_nomenclature_item_id": "0101210010",
                    "producline_suffix": "80",
                    "description": "First",
                },
            },
            {
                "type": "commodity",
                "attributes": {
                    "goods_nomenclature_item_id": "0101210020",
                    "producline_suffix": "80",
                    "description": "Second (same CN-8)",
                },
            },
        ]}
        rows = extract_cn8_codes(payload)
        assert len(rows) == 1  # both share CN-8 01012100
        assert rows[0]["code"] == "01012100"

    def test_extract_cn8_codes_parent_link(self):
        rows = extract_cn8_codes(HEADING_PAYLOAD)
        for r in rows:
            assert r["parent_code"] == r["code"][:4]

    def test_extract_cn8_codes_skips_short(self):
        payload = {"included": [
            {
                "type": "commodity",
                "attributes": {
                    "goods_nomenclature_item_id": "010",
                    "producline_suffix": "80",
                    "description": "too short",
                },
            },
        ]}
        assert extract_cn8_codes(payload) == []


class TestEuCnInsert:
    def test_insert_codes_basic(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.rowcount = 1

        rows = [{
            "code": "01012100",
            "code_type": "EU_CN_8",
            "description": "Horses",
            "parent_code": "0101",
            "country_scope": "EU",
        }]

        count = insert_codes(rows, conn)
        assert count == 1
        cur.execute.assert_called_once()
        conn.commit.assert_called_once()

    def test_insert_codes_rolls_back(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.execute.side_effect = Exception("db down")

        with pytest.raises(Exception, match="db down"):
            insert_codes(
                [{"code": "x", "code_type": "EU_CN_8",
                  "description": "x", "parent_code": None, "country_scope": "EU"}],
                conn,
            )
        conn.rollback.assert_called_once()


class TestEuCnLoadEnd2End:
    @patch("hs_library.load_eu_cn.httpx.Client")
    def test_load_eu_cn_with_filter(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        # Set up the sequence: chapters list, chapter detail, heading details
        chapters_resp = MagicMock()
        chapters_resp.json.return_value = {
            "data": [
                {"attributes": {"goods_nomenclature_item_id": "0100000000"}},
                {"attributes": {"goods_nomenclature_item_id": "0200000000"}},
            ],
        }
        chapter_resp = MagicMock()
        chapter_resp.json.return_value = CHAPTER_PAYLOAD
        heading_resp = MagicMock()
        heading_resp.json.return_value = HEADING_PAYLOAD

        # Three calls per included chapter: chapters list (once), chapter detail, then 2 heading details
        mock_client.get.side_effect = [
            chapters_resp,
            chapter_resp,
            heading_resp,
            heading_resp,
        ]

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.rowcount = 1

        result = load_eu_cn_codes(conn, chapter_filter=["01"])

        # 2 headings per chapter, 2 CN-8 codes per heading = 4 codes
        assert result["headings"] == 2
        assert result["cn8_codes"] == 4
        assert result["failed_headings"] == 0

    @patch("hs_library.load_eu_cn.httpx.Client")
    def test_load_eu_cn_handles_heading_failure(self, mock_client_cls):
        import httpx as _httpx

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        chapters_resp = MagicMock()
        chapters_resp.json.return_value = {
            "data": [{"attributes": {"goods_nomenclature_item_id": "0100000000"}}],
        }
        chapter_resp = MagicMock()
        chapter_resp.json.return_value = CHAPTER_PAYLOAD

        # First heading fetch succeeds, second raises HTTP error
        good_heading_resp = MagicMock()
        good_heading_resp.json.return_value = HEADING_PAYLOAD

        mock_client.get.side_effect = [
            chapters_resp,
            chapter_resp,
            good_heading_resp,
            _httpx.HTTPError("boom"),
        ]

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.rowcount = 1

        result = load_eu_cn_codes(conn, chapter_filter=["01"])
        assert result["failed_headings"] == 1
        assert result["headings"] == 2
