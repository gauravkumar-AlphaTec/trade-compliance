"""Tests for kb/sources/nando_pdf.py.

Uses three real fixture PDFs from data/Europa docs Germany/, one per
template variant (Machinery, MDR, CPR), to verify that the parser
extracts the same canonical fields across templates with structural
differences.
"""
import os
from pathlib import Path

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

from kb.sources.nando_pdf import _parse_directive_ref, extract_notification

PDF_DIR = Path(__file__).parent.parent / "data" / "Europa docs Germany"


def test_parse_directive_ref_handles_known_formats():
    cases = {
        "2006/42/EC Machinery": "2006/42/EC",
        "Regulation (EU) 2017/745 on medical devices": "2017/745",
        "Regulation (EU) No 305/2011 - Construction products": "305/2011",
        "90/385/EEC Active implantable medical devices": "90/385/EEC",
        "Regulation (EU) 2023/1230 on machinery": "2023/1230",
    }
    for name, expected in cases.items():
        assert _parse_directive_ref(name) == expected, name


def test_parse_directive_ref_returns_none_for_no_match():
    assert _parse_directive_ref(None) is None
    assert _parse_directive_ref("no directive number here") is None


@pytest.mark.skipif(
    not (PDF_DIR / "2006_42_EC_notification.pdf").exists(),
    reason="fixture PDFs not present (data/Europa docs Germany/)",
)
def test_extract_machinery_template_full_fields():
    """Old-style directive PDF: has accreditation block AND standards block."""
    rec = extract_notification(PDF_DIR / "2006_42_EC_notification.pdf")
    assert rec["nb_number"].isdigit() and len(rec["nb_number"]) == 4
    assert rec["directive_ref"] == "2006/42/EC"
    assert rec["notifying_authority"] == "ZLS"
    assert rec["accreditation_body"] is not None
    assert "DAkkS" in rec["accreditation_body"]
    assert rec["assessment_standards"]  # non-empty
    assert rec["last_approval_date"] is not None


@pytest.mark.skipif(
    not (PDF_DIR / "Regulation (EU) 2017_745_notification.pdf").exists(),
    reason="fixture PDFs not present",
)
def test_extract_mdr_template_tolerates_missing_blocks():
    """MDR template omits the accreditation + standards blocks — parser must not crash."""
    rec = extract_notification(PDF_DIR / "Regulation (EU) 2017_745_notification.pdf")
    assert rec["nb_number"].isdigit()
    assert rec["directive_ref"] == "2017/745"
    assert rec["notifying_authority"] == "ZLG"  # medical authority, not ZLS
    assert rec["accreditation_body"] is None
    assert rec["assessment_standards"] == []


@pytest.mark.skipif(
    not (PDF_DIR / "Regulation (EU) 305_2011_notification.pdf").exists(),
    reason="fixture PDFs not present",
)
def test_extract_cpr_template_with_dibt_authority():
    """CPR uses DIBt (mixed case acronym) as the notifying authority."""
    rec = extract_notification(PDF_DIR / "Regulation (EU) 305_2011_notification.pdf")
    assert rec["directive_ref"] == "305/2011"
    assert rec["notifying_authority"] == "DIBt"
