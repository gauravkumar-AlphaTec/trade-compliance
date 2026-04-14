"""Parse a NANDO/SMCS notification PDF into a structured dict.

The PDFs are machine-generated from a template. Layout varies between
directives — the MDR/IVDR templates omit the accreditation and standards
blocks that the older "New Approach" directive templates include — so the
parser tolerates missing fields rather than asserting on them.

All canonical fields (including directive_ref) are read from the PDF
body. Filenames are not trusted: identical filenames with " (1)", " (2)"
suffixes are *different* notifications captured by the browser's
download de-duplication, not duplicates of the same body.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pdfplumber


_NB_RE = re.compile(r"\bNB\s*(\d{4})\b")
_DIRECTIVE_REF_RE = re.compile(r"\b(\d{2,4}/\d{1,4}(?:/[A-Z]{2,3})?)\b")
_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_AUTHORITY_ACRONYM_RE = re.compile(r"\(([A-Z][A-Za-z]{1,6})\)")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")
_URL_RE = re.compile(r"(https?://\S+|www\.\S+)")


def _parse_directive_ref(directive_name: str | None) -> str | None:
    """Pull a canonical reference like '2006/42/EC' or '305/2011' from the Legislation: line."""
    if not directive_name:
        return None
    m = _DIRECTIVE_REF_RE.search(directive_name)
    return m.group(1) if m else None


def _read_first_page_lines(path: Path) -> list[str]:
    with pdfplumber.open(str(path)) as pdf:
        text = pdf.pages[0].extract_text() or ""
    return [ln.rstrip() for ln in text.splitlines()]


def _line_after(lines: list[str], label: str, *, contains: bool = True) -> str | None:
    """Return the first non-empty line after the line containing `label`."""
    for i, ln in enumerate(lines):
        hit = (label in ln) if contains else (ln.strip() == label)
        if hit:
            for j in range(i + 1, len(lines)):
                if lines[j].strip():
                    return lines[j].strip()
    return None


def _block_after(lines: list[str], label: str, max_lines: int = 12) -> list[str]:
    """Return up to `max_lines` non-blank lines after the labelled line, stopping at the next labelled line."""
    out: list[str] = []
    started = False
    for ln in lines:
        if not started:
            if label in ln:
                started = True
            continue
        s = ln.strip()
        if not s:
            if out:
                break
            continue
        # Heuristic: a line ending with ':' marks the next field block.
        if s.endswith(":") and out:
            break
        out.append(s)
        if len(out) >= max_lines:
            break
    return out


def _parse_authority(lines: list[str]) -> str | None:
    """Pull the acronym from the 'From:' header (e.g. 'ZLS', 'ZLG', 'BfArM')."""
    for i, ln in enumerate(lines):
        if ln.strip().startswith("From:"):
            window = " ".join(lines[i : i + 6])
            m = _AUTHORITY_ACRONYM_RE.search(window)
            if m:
                return m.group(1)
            break
    return None


def _parse_directive_name(lines: list[str]) -> str | None:
    for ln in lines:
        if "Legislation:" in ln:
            tail = ln.split("Legislation:", 1)[1].strip()
            return tail or None
    return None


def _parse_approval_date(lines: list[str]) -> date | None:
    for ln in lines:
        if "approval date" in ln.lower():
            m = _DATE_RE.search(ln)
            if m:
                return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    return None


def _parse_standards(lines: list[str]) -> list[str]:
    """The 'assessed according to:' block lists ISO/IEC 17xxx standards, comma-separated and possibly across lines."""
    block = _block_after(lines, "assessed according to", max_lines=6)
    if not block:
        return []
    joined = " ".join(block)
    # Strip trailing 'Page N' footer that pdfplumber sometimes pulls into the last block.
    joined = re.sub(r"\bPage\s*\d+\s*$", "", joined).strip()
    parts = [p.strip(" .;") for p in re.split(r"[,;]", joined)]
    # Keep things that look like a standard reference (contain a digit and a letter run).
    return [p for p in parts if p and re.search(r"\d", p) and len(p) <= 80]


def _parse_body_block(lines: list[str]) -> dict[str, str | None]:
    """Extract the body name + city + email + website from the 'Body name, address...' block."""
    block = _block_after(lines, "Body name, address", max_lines=10)
    name = block[0] if block else None
    email = next((m.group(0) for ln in block if (m := _EMAIL_RE.search(ln))), None)
    website = next((m.group(0) for ln in block if (m := _URL_RE.search(ln))), None)
    # City: the line immediately before "Germany" (or whatever country line precedes the phone).
    city = None
    for i, ln in enumerate(block[1:], start=1):
        if ln.startswith("+") and i >= 2:
            # block[i-1] is the country line (e.g. 'Germany'); block[i-2] is the city/postcode line.
            candidate = block[i - 2] if i >= 2 else None
            if candidate and "@" not in candidate and "http" not in candidate.lower():
                city = candidate
            break
    return {"name": name, "city": city, "email": email, "website": website}


def extract_notification(path: Path) -> dict:
    """Parse a single notification PDF. Returns {} if unparseable."""
    path = Path(path)
    lines = _read_first_page_lines(path)
    if not lines:
        return {}

    nb = next((m.group(1) for ln in lines if (m := _NB_RE.search(ln))), None)
    if not nb:
        return {}

    body = _parse_body_block(lines)
    accreditation = _line_after(lines, "National Accreditation Body")
    directive_name = _parse_directive_name(lines)

    return {
        "nb_number": nb,
        "name": body["name"],
        "city": body["city"],
        "email": body["email"],
        "website": body["website"],
        "directive_ref": _parse_directive_ref(directive_name),
        "directive_name": directive_name,
        "notifying_authority": _parse_authority(lines),
        "last_approval_date": _parse_approval_date(lines),
        "accreditation_body": accreditation,
        "assessment_standards": _parse_standards(lines),
        "source_file": path.name,
    }
