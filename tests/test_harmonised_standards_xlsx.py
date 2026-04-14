"""Tests for kb/sources/harmonised_standards_xlsx.py.

Covers both XLSX schemas the EU Commission ships:
  - Schema A (Machinery, MDR): 'Reference and title' combined column,
    'Start of legal effect' / 'End of legal effect' dates.
  - Schema B (LVD, EMC, RoHS): separate code + title columns,
    'Date of start of presumption of conformity' dates.
"""
import os
from pathlib import Path

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

from kb.sources.harmonised_standards_xlsx import (
    _split_combined_ref_title,
    directive_from_filename,
    parse_xlsx,
)

XLSX_DIR = Path(__file__).parent.parent / "data" / "harmonised_standards"


def test_split_combined_ref_title():
    code, title = _split_combined_ref_title(
        "EN ISO 12100:2010\nSafety of machinery — General principles for design"
    )
    assert code == "EN ISO 12100:2010"
    assert title.startswith("Safety of machinery")


def test_split_combined_ref_title_handles_no_newline():
    assert _split_combined_ref_title("EN 60204-1:2018") == ("EN 60204-1:2018", None)


def test_directive_from_filename():
    assert directive_from_filename("2006_42_EC.xlsx") == "2006/42/EC"
    assert directive_from_filename("2017_745.xlsx") == "2017/745"
    assert directive_from_filename("2011_65_EU.xlsx") == "2011/65/EU"


@pytest.mark.skipif(
    not (XLSX_DIR / "2006_42_EC.xlsx").exists(),
    reason="fixture XLSX not present",
)
def test_parse_schema_a_machinery():
    """Schema A (combined ref+title column, Start/End of legal effect)."""
    recs = parse_xlsx(XLSX_DIR / "2006_42_EC.xlsx")
    assert len(recs) > 100
    sample = recs[0]
    assert sample["directive_ref"] == "2006/42/EC"
    assert sample["standard_code"].startswith("EN ")
    assert sample["eso"] in {"CEN", "Cenelec", "ETSI"}
    # Some rows must have an in-force-from date.
    assert any(r["in_force_from"] for r in recs)


@pytest.mark.skipif(
    not (XLSX_DIR / "2014_35_EU.xlsx").exists(),
    reason="fixture XLSX not present",
)
def test_parse_schema_b_lvd():
    """Schema B (separate code + title columns, presumption-of-conformity dates)."""
    recs = parse_xlsx(XLSX_DIR / "2014_35_EU.xlsx")
    assert len(recs) > 100
    sample = recs[0]
    assert sample["directive_ref"] == "2014/35/EU"
    assert sample["standard_code"]
    assert sample["title"]
    # Mix of in-force and withdrawn rows.
    assert any(r["withdrawn_on"] is None for r in recs)
    assert any(r["withdrawn_on"] is not None for r in recs)


@pytest.mark.skipif(
    not (XLSX_DIR / "2011_65_EU.xlsx").exists(),
    reason="fixture XLSX not present",
)
def test_parse_legacy_xls_format():
    """RoHS file is served as legacy .xls inside an .xlsx-named file — handled via xlrd fallback."""
    recs = parse_xlsx(XLSX_DIR / "2011_65_EU.xlsx")
    assert len(recs) >= 1
    assert all(r["directive_ref"] == "2011/65/EU" for r in recs)
