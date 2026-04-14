"""Parse an EU Commission harmonised-standards "Summary list" XLSX.

The Commission publishes one XLSX per directive at:
    https://single-market-economy.ec.europa.eu/single-market/goods/
        european-standards/harmonised-standards/<directive-slug>_en

Two schemas exist in the wild:

A. Newer (Machinery 2006/42/EC, MDR 2017/745):
   columns include 'Reference and title Provision' (code + title combined,
   newline-separated) and 'Start of legal effect' / 'End of legal effect'.

B. Older (RoHS 2011/65/EU, EMC 2014/30/EU, LVD 2014/35/EU):
   separate 'Reference number of the standard (C)' and 'Title of the
   standard (D)' columns; dates are 'Date of start of presumption of
   conformity' and 'Date of withdrawal from OJ ...'. Some files arrive
   in the legacy .xls binary format (handled via xlrd).

The parser auto-detects schema by header content and emits a single
normalised dict per row.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# Headers we look for to disambiguate schema A vs B.
_REF_TITLE_COMBINED = "Reference and title"      # schema A
_REF_NUMBER_SEP = "Reference number of the standard"  # schema B


def _read_excel_any(path: Path) -> pd.DataFrame:
    """Read an .xlsx (openpyxl) or legacy .xls (xlrd), returning the first sheet, no header."""
    try:
        return pd.read_excel(path, header=None, engine="openpyxl")
    except Exception:
        return pd.read_excel(path, header=None, engine="xlrd")


def _find_header_row(df: pd.DataFrame) -> int:
    """Return the 0-indexed row containing 'Legislation' in column A."""
    for i, val in enumerate(df.iloc[:, 0]):
        if isinstance(val, str) and "Legislation" in val:
            return i
    raise ValueError("No header row containing 'Legislation' found")


def _to_date(value) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        s = value.strip()
        if not s or s == "-":
            return None
        # Try a couple of common shapes.
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _clean_str(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s == "-":
        return None
    return s


def _split_combined_ref_title(cell: str) -> tuple[str | None, str | None]:
    """Schema A: 'EN 81-31:2010\\nSafety rules ...' -> ('EN 81-31:2010', 'Safety rules ...')."""
    if not cell:
        return None, None
    parts = cell.split("\n", 1)
    code = parts[0].strip() or None
    title = parts[1].strip() if len(parts) > 1 else None
    return code, title


def directive_from_filename(name: str) -> str | None:
    """`2006_42_EC.xlsx` -> '2006/42/EC'.  `2017_745.xlsx` -> '2017/745'."""
    stem = Path(name).stem
    if not re.match(r"^[\w_]+$", stem):
        return None
    return stem.replace("_", "/")


def parse_xlsx(path: Path) -> list[dict]:
    """Return a list of normalised harmonised-standard records from one XLSX."""
    path = Path(path)
    df = _read_excel_any(path)
    header_idx = _find_header_row(df)
    headers = [str(h) if h is not None else "" for h in df.iloc[header_idx]]
    body = df.iloc[header_idx + 1 :].reset_index(drop=True)
    body.columns = headers

    # Detect schema.
    if any(_REF_TITLE_COMBINED in h for h in headers):
        schema = "A"
    elif any(_REF_NUMBER_SEP in h for h in headers):
        schema = "B"
    else:
        raise ValueError(f"Unknown schema in {path.name}: headers={headers}")

    fallback_directive = directive_from_filename(path.name)
    records: list[dict] = []

    eso_col = next((h for h in headers if h.startswith("ESO")), None)
    ref_num_col = next((h for h in headers if _REF_NUMBER_SEP in h), None)
    title_col = next((h for h in headers if h.startswith("Title of the standard")), None)
    in_force_col_b = next((h for h in headers if "start of presumption of conformity" in h), None)
    withdrawn_col_b = next((h for h in headers if "Date of withdrawal from OJ" in h), None)
    oj_pub_col_b = next((h for h in headers if "publication in OJ" in h), None)
    oj_wd_col_b = next((h for h in headers if "withdrawal from OJ (7)" in h), None)
    combined_col = next((h for h in headers if _REF_TITLE_COMBINED in h), None)

    for _, row in body.iterrows():
        eso = _clean_str(row.get(eso_col)) if eso_col else None
        directive_cell = _clean_str(row.get(headers[0]))
        # 'Legislation' values: '2006/42/EC - Machinery (MD)' (schema A) or '2014/35/EU' (schema B).
        directive_ref = None
        if directive_cell:
            m = re.search(r"(\d{2,4}/\d+(?:/[A-Z]{2,3})?)", directive_cell)
            if m:
                directive_ref = m.group(1)
        directive_ref = directive_ref or fallback_directive

        if schema == "A":
            code, title = _split_combined_ref_title(_clean_str(row.get(combined_col)) or "") if combined_col else (None, None)
            in_force_from = _to_date(row.get("Start of legal effect"))
            withdrawn_on = _to_date(row.get("End of legal effect"))
            oj_pub = _clean_str(row.get("Publication OJ reference"))
            oj_wd = _clean_str(row.get("Withdrawal OJ reference"))
        else:
            code = _clean_str(row.get(ref_num_col)) if ref_num_col else None
            title = _clean_str(row.get(title_col)) if title_col else None
            in_force_from = _to_date(row.get(in_force_col_b)) if in_force_col_b else None
            withdrawn_on = _to_date(row.get(withdrawn_col_b)) if withdrawn_col_b else None
            oj_pub = _clean_str(row.get(oj_pub_col_b)) if oj_pub_col_b else None
            oj_wd = _clean_str(row.get(oj_wd_col_b)) if oj_wd_col_b else None

        if not code or not directive_ref:
            continue

        records.append({
            "standard_code": code,
            "title": title,
            "eso": eso,
            "directive_ref": directive_ref,
            "in_force_from": in_force_from,
            "withdrawn_on": withdrawn_on,
            "oj_publication_ref": oj_pub,
            "oj_withdrawal_ref": oj_wd,
        })

    return records
