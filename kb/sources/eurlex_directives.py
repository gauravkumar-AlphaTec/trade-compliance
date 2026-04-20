"""Fetch EU directive metadata and scope text from the CELLAR repository.

CELLAR is the EU Publications Office's content store.  Content
negotiation on ``http://publications.europa.eu/resource/celex/{celex}``
returns the full directive text as XHTML when the Accept header asks for
``application/xhtml+xml`` and ``Accept-Language: en``.

No API key needed.  Rate-limit: keep ≤ 1 req / second.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

CELLAR_BASE = "http://publications.europa.eu/resource/celex"
TIMEOUT = 60


@dataclass
class DirectiveInfo:
    celex: str
    title: str = ""
    scope_text: str = ""
    definitions_text: str = ""
    product_categories: list[str] = field(default_factory=list)
    exclusions: list[str] = field(default_factory=list)
    full_text_length: int = 0
    fetch_ok: bool = False
    error: str = ""


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _find_article(text: str, article_num: int, max_chars: int = 3000) -> str:
    """Find 'Article N' followed by its content, up to the next Article heading."""
    pattern = rf"Article\s+{article_num}\b"
    best = ""
    for m in re.finditer(pattern, text):
        chunk = text[m.start() : m.start() + max_chars]
        # Trim at the next article heading.
        next_art = re.search(
            rf"Article\s+{article_num + 1}\b", chunk[20:]
        )
        if next_art:
            chunk = chunk[: 20 + next_art.start()]
        # Keep the longest match that contains scope-like keywords.
        if any(
            w in chunk.lower()
            for w in ("scope", "subject", "shall apply", "applies to", "definition")
        ):
            if len(chunk) > len(best):
                best = chunk
    return best.strip()


def fetch_directive(celex: str) -> DirectiveInfo:
    """Fetch a single directive's full text from CELLAR and extract scope."""
    info = DirectiveInfo(celex=celex)
    url = f"{CELLAR_BASE}/{celex}"

    try:
        r = httpx.get(
            url,
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={
                "Accept": "application/xhtml+xml, text/html",
                "Accept-Language": "en",
            },
        )
        if r.status_code != 200 or not r.text:
            info.error = f"HTTP {r.status_code}"
            return info
    except httpx.HTTPError as exc:
        info.error = str(exc)
        return info

    info.full_text_length = len(r.text)
    text = _strip_html(r.text)

    # Title: usually near the start, after preamble identifiers.
    title_match = re.search(
        r"((?:DIRECTIVE|REGULATION)\s+\((?:EU|EC)\)\s+\S+\s+OF\s+THE\s+EUROPEAN\s+PARLIAMENT[^.]{10,300})",
        text,
        re.IGNORECASE,
    )
    if title_match:
        info.title = title_match.group(1).strip()

    # Article 1 — scope / subject matter.
    info.scope_text = _find_article(text, 1)
    # Article 2 — definitions (often has product-category list).
    info.definitions_text = _find_article(text, 2)

    info.fetch_ok = True
    return info


def fetch_all_directives(
    celex_list: list[str], delay: float = 1.5
) -> list[DirectiveInfo]:
    """Fetch multiple directives with a polite delay between requests."""
    results: list[DirectiveInfo] = []
    for i, celex in enumerate(celex_list):
        logger.info("Fetching %s (%d/%d)", celex, i + 1, len(celex_list))
        info = fetch_directive(celex)
        results.append(info)
        if info.fetch_ok:
            logger.info(
                "  OK: %d bytes, scope %d chars",
                info.full_text_length,
                len(info.scope_text),
            )
        else:
            logger.warning("  FAIL: %s", info.error)
        if i < len(celex_list) - 1:
            time.sleep(delay)
    return results


# ------------------------------------------------------------------
# Known NLF product-safety directives/regulations (CELEX numbers).
# 'L' = Directive, 'R' = Regulation.  Prefix '3' = EU act.
# ------------------------------------------------------------------
NLF_DIRECTIVES: dict[str, dict] = {
    "32006L0042": {
        "short": "Machinery",
        "directive_ref": "2006/42/EC",
        "effective_date": "2009-12-29",
    },
    "32014L0035": {
        "short": "LVD",
        "directive_ref": "2014/35/EU",
        "effective_date": "2016-04-20",
    },
    "32014L0030": {
        "short": "EMC",
        "directive_ref": "2014/30/EU",
        "effective_date": "2016-04-20",
    },
    "32017R0745": {
        "short": "MDR",
        "directive_ref": "2017/745",
        "effective_date": "2021-05-26",
    },
    "32011L0065": {
        "short": "RoHS",
        "directive_ref": "2011/65/EU",
        "effective_date": "2013-01-02",
    },
    "32014L0053": {
        "short": "RED",
        "directive_ref": "2014/53/EU",
        "effective_date": "2016-06-13",
    },
    "32014L0068": {
        "short": "PED",
        "directive_ref": "2014/68/EU",
        "effective_date": "2016-07-19",
    },
    "32009L0048": {
        "short": "Toy Safety",
        "directive_ref": "2009/48/EC",
        "effective_date": "2011-07-20",
    },
    "32016R0425": {
        "short": "PPE",
        "directive_ref": "2016/425",
        "effective_date": "2018-04-21",
    },
    "32016R0426": {
        "short": "Gas Appliances",
        "directive_ref": "2016/426",
        "effective_date": "2018-04-21",
    },
    "32014L0034": {
        "short": "ATEX",
        "directive_ref": "2014/34/EU",
        "effective_date": "2016-04-20",
    },
    "32014L0033": {
        "short": "Lifts",
        "directive_ref": "2014/33/EU",
        "effective_date": "2016-04-20",
    },
    "32014L0032": {
        "short": "Measuring Instruments",
        "directive_ref": "2014/32/EU",
        "effective_date": "2016-04-20",
    },
    "32013L0053": {
        "short": "Recreational Craft",
        "directive_ref": "2013/53/EU",
        "effective_date": "2016-01-18",
    },
    "32014L0029": {
        "short": "Simple Pressure Vessels",
        "directive_ref": "2014/29/EU",
        "effective_date": "2016-04-20",
    },
    "32014L0090": {
        "short": "Marine Equipment",
        "directive_ref": "2014/90/EU",
        "effective_date": "2016-09-18",
    },
    # --- NANDO-referenced directives not in the first batch ---
    "32011R0305": {
        "short": "Construction Products",
        "directive_ref": "305/2011",
        "effective_date": "2013-07-01",
    },
    "32023R1230": {
        "short": "Machinery Regulation (new)",
        "directive_ref": "2023/1230",
        "effective_date": "2027-01-20",
    },
    "32014L0031": {
        "short": "NAWI",
        "directive_ref": "2014/31/EU",
        "effective_date": "2016-04-20",
    },
    "32010L0035": {
        "short": "Transportable Pressure Equipment",
        "directive_ref": "2010/35/EU",
        "effective_date": "2011-06-21",
    },
    "32000L0014": {
        "short": "Outdoor Noise",
        "directive_ref": "2000/14/EC",
        "effective_date": "2002-01-03",
    },
    "32016L0797": {
        "short": "Railway Interoperability",
        "directive_ref": "2016/797",
        "effective_date": "2016-06-15",
    },
    "32017R0746": {
        "short": "IVDR",
        "directive_ref": "2017/746",
        "effective_date": "2022-05-26",
    },
    "32016R0424": {
        "short": "Cableway Installations",
        "directive_ref": "2016/424",
        "effective_date": "2018-04-21",
    },
    "32013L0029": {
        "short": "Pyrotechnic Articles",
        "directive_ref": "2013/29/EU",
        "effective_date": "2015-07-01",
    },
    "32014L0028": {
        "short": "Explosives for Civil Use",
        "directive_ref": "2014/28/EU",
        "effective_date": "2016-04-20",
    },
    "32019R0945": {
        "short": "Drones (UAS)",
        "directive_ref": "2019/945",
        "effective_date": "2019-07-01",
    },
    # --- Legacy directives still active in NANDO ---
    "31993L0042": {
        "short": "MDD (legacy)",
        "directive_ref": "93/42/EEC",
        "effective_date": "1998-06-14",
    },
    "31990L0385": {
        "short": "AIMDD (legacy)",
        "directive_ref": "90/385/EEC",
        "effective_date": "1995-01-01",
    },
    "31998L0079": {
        "short": "IVDD (legacy)",
        "directive_ref": "98/79/EC",
        "effective_date": "2003-12-07",
    },
    "31992L0042": {
        "short": "Boiler Efficiency (legacy)",
        "directive_ref": "92/42/EEC",
        "effective_date": "1994-01-01",
    },
    # --- Additional significant EU product directives ---
    "32009L0125": {
        "short": "Ecodesign (ErP)",
        "directive_ref": "2009/125/EC",
        "effective_date": "2009-11-20",
    },
    "32010L0030": {
        "short": "Energy Labelling",
        "directive_ref": "2010/30/EU",
        "effective_date": "2010-06-18",
    },
    "32014L0094": {
        "short": "Alt Fuels Infrastructure",
        "directive_ref": "2014/94/EU",
        "effective_date": "2016-11-18",
    },
}
