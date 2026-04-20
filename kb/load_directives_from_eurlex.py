"""Load EU NLF directives from CELLAR and seed regulations + HS mappings.

Pipeline:
    1. Fetch directive full text from CELLAR (no API key, rate ~1 req/s)
    2. Extract scope + definitions via regex
    3. (Optional) Use LLM to derive 4-digit HS headings from scope text
    4. Insert/update regulations + regulation_hs_codes with directive_ref

Without --use-llm, uses a built-in fallback mapping of directive → HS
headings curated from the directive scope text.

Usage:
    # With LLM (requires Ollama running):
    python -m kb.load_directives_from_eurlex --use-llm

    # Without LLM (uses built-in fallback):
    python -m kb.load_directives_from_eurlex
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2

from kb.sources.eurlex_directives import (
    DirectiveInfo,
    NLF_DIRECTIVES,
    fetch_all_directives,
)

logger = logging.getLogger(__name__)

CONFIDENCE = 0.90
SOURCE_NAME = "EUR-Lex"

# ------------------------------------------------------------------
# Fallback heading mappings — curated from directive scope text.
# Used when Ollama is not available.
# ------------------------------------------------------------------
FALLBACK_HS_HEADINGS: dict[str, list[str]] = {
    "2006/42/EC": [  # Machinery
        "8207", "8408", "8410", "8411", "8412", "8413", "8414", "8415",
        "8416", "8417", "8418", "8419", "8420", "8421", "8422", "8423",
        "8424", "8425", "8426", "8427", "8428", "8429", "8430", "8431",
        "8432", "8433", "8434", "8435", "8436", "8437", "8438", "8439",
        "8440", "8441", "8442", "8443", "8444", "8445", "8446", "8447",
        "8448", "8449", "8450", "8451", "8452", "8453", "8454", "8455",
        "8456", "8457", "8458", "8459", "8460", "8461", "8462", "8463",
        "8464", "8465", "8466", "8467", "8468", "8474", "8475", "8477",
        "8478", "8479", "8480",
    ],
    "2014/35/EU": [  # LVD — electrical equipment 50-1000V AC / 75-1500V DC
        "8501", "8502", "8504", "8508", "8509", "8510", "8513", "8514",
        "8515", "8516", "8517", "8519", "8521", "8525", "8527", "8528",
        "8529", "8531", "8535", "8536", "8537", "8538", "8539", "8540",
        "8541", "8543", "8545", "9405",
    ],
    "2014/30/EU": [  # EMC
        "8501", "8504", "8509", "8516", "8517", "8518", "8519", "8521",
        "8525", "8527", "8528", "8531", "8536", "8537", "8543",
    ],
    "2017/745": [  # MDR
        "9018", "9019", "9020", "9021", "9022", "9402",
    ],
    "2011/65/EU": [  # RoHS
        "8501", "8504", "8509", "8516", "8517", "8518", "8519", "8521",
        "8525", "8527", "8528", "8536", "8539", "8543", "9405", "9504",
    ],
    "2014/53/EU": [  # RED — radio equipment
        "8517", "8518", "8519", "8525", "8526", "8527", "8528", "8529",
        "8543",
    ],
    "2014/68/EU": [  # PED — pressure equipment > 0.5 bar
        "7304", "7305", "7306", "7309", "7310", "7311", "7613",
        "8402", "8403", "8404", "8419", "8481",
    ],
    "2009/48/EC": [  # Toy Safety
        "9503", "9504", "9505",
    ],
    "2016/425": [  # PPE
        "3926", "4015", "6116", "6210", "6216", "6307",
        "6402", "6403", "6404", "6406", "6505", "6506",
        "9004", "9020",
    ],
    "2016/426": [  # Gas Appliances
        "7321", "7322", "8416", "8419",
    ],
    "2014/34/EU": [  # ATEX
        "8413", "8414", "8501", "8504", "8531", "8536", "8537", "8543",
        "9026", "9027", "9028",
    ],
    "2014/33/EU": [  # Lifts
        "8428", "8431",
    ],
    "2014/32/EU": [  # Measuring Instruments
        "8423", "9016", "9017", "9025", "9026", "9028", "9029", "9030",
        "9031", "9032",
    ],
    "2013/53/EU": [  # Recreational Craft
        "8903",
    ],
    "2014/29/EU": [  # Simple Pressure Vessels
        "7311", "7613",
    ],
    "2014/90/EU": [  # Marine Equipment
        "7326", "8526", "8531", "8906", "8907", "9014", "9015",
        "9029", "9030",
    ],
    # --- NANDO-referenced directives ---
    "305/2011": [  # Construction Products
        "2523", "3214", "3816", "3824", "3925", "4410", "4411", "4412",
        "6807", "6810", "6811", "6901", "6902", "6904", "6905", "6907",
        "7003", "7004", "7005", "7007", "7008", "7019", "7210", "7216",
        "7301", "7308", "7604", "7610", "8481",
    ],
    "2023/1230": [  # New Machinery Regulation — same scope as 2006/42/EC
        "8207", "8408", "8410", "8411", "8412", "8413", "8414", "8415",
        "8416", "8417", "8418", "8419", "8420", "8421", "8422", "8423",
        "8424", "8425", "8426", "8427", "8428", "8429", "8430", "8431",
        "8432", "8433", "8434", "8435", "8436", "8437", "8438", "8439",
        "8440", "8441", "8442", "8443", "8444", "8445", "8446", "8447",
        "8448", "8449", "8450", "8451", "8452", "8453", "8454", "8455",
        "8456", "8457", "8458", "8459", "8460", "8461", "8462", "8463",
        "8464", "8465", "8466", "8467", "8468", "8474", "8475", "8477",
        "8478", "8479", "8480",
    ],
    "2014/31/EU": [  # NAWI — non-automatic weighing instruments
        "8423",
    ],
    "2010/35/EU": [  # Transportable Pressure Equipment
        "7311", "7613", "8609",
    ],
    "2000/14/EC": [  # Outdoor Noise
        "8429", "8430", "8432", "8433", "8436", "8467", "8508",
    ],
    "2016/797": [  # Railway Interoperability
        "8601", "8602", "8603", "8604", "8605", "8606", "8607", "8608",
        "8609",
    ],
    "2017/746": [  # IVDR — in vitro diagnostics
        "3002", "3822", "9018", "9027",
    ],
    "2016/424": [  # Cableway Installations
        "8428",
    ],
    "2013/29/EU": [  # Pyrotechnic Articles
        "3604",
    ],
    "2014/28/EU": [  # Explosives for Civil Use
        "3601", "3602", "3603",
    ],
    "2019/945": [  # Drones (UAS)
        "8525", "8802",
    ],
    # --- Legacy directives (superseded but NBs still active) ---
    "93/42/EEC": [  # MDD (legacy, superseded by MDR 2017/745)
        "9018", "9019", "9021", "9022",
    ],
    "90/385/EEC": [  # AIMDD (legacy, superseded by MDR)
        "9018", "9021",
    ],
    "98/79/EC": [  # IVDD (legacy, superseded by IVDR 2017/746)
        "3002", "3822", "9018", "9027",
    ],
    "92/42/EEC": [  # Hot-water boiler efficiency (legacy)
        "8403",
    ],
    # --- Additional significant EU product directives ---
    "2009/125/EC": [  # Ecodesign (ErP framework)
        "8413", "8414", "8415", "8418", "8450", "8501", "8504", "8508",
        "8509", "8516", "8528", "8539", "9405",
    ],
    "2010/30/EU": [  # Energy Labelling
        "8415", "8418", "8422", "8450", "8451", "8508", "8516", "8528",
        "9405",
    ],
    "2014/94/EU": [  # Alternative Fuels Infrastructure
        "8504", "8543",
    ],
}


def _upsert_regulation(
    conn,
    celex: str,
    directive_meta: dict,
    directive_info: DirectiveInfo,
    now: datetime,
) -> int:
    """Upsert a source + regulation row. Returns the regulation id."""
    cur = conn.cursor()
    try:
        # Source row.
        url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
        cur.execute(
            """
            INSERT INTO sources (source_name, document_id, url, fetched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_name, document_id) DO UPDATE SET
                url = EXCLUDED.url, fetched_at = EXCLUDED.fetched_at
            RETURNING id
            """,
            (SOURCE_NAME, celex, url, now),
        )
        source_id = cur.fetchone()[0]

        # Build title from CELLAR or short name.
        title = directive_info.title or ""
        if not title:
            short = directive_meta.get("short", "")
            dref = directive_meta["directive_ref"]
            doc_type = "Regulation" if "R" in celex[5] else "Directive"
            title = f"{doc_type} {dref} — {short}"

        doc_type = "regulation" if "R" in celex[5] else "directive"
        summary = directive_info.scope_text[:1000] if directive_info.scope_text else None

        cur.execute(
            """
            INSERT INTO regulations (
                source_id, title, document_type, authority, country,
                effective_date, summary, status, directive_ref
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s)
            ON CONFLICT (source_id) DO UPDATE SET
                title = EXCLUDED.title,
                document_type = EXCLUDED.document_type,
                directive_ref = EXCLUDED.directive_ref,
                summary = EXCLUDED.summary,
                updated_at = NOW()
            RETURNING id
            """,
            (
                source_id,
                title,
                doc_type,
                "European Commission",
                "DE",
                directive_meta.get("effective_date"),
                summary,
                directive_meta["directive_ref"],
            ),
        )
        reg_id = cur.fetchone()[0]
        return reg_id
    finally:
        cur.close()


def _link_hs_headings(
    conn,
    reg_id: int,
    headings: list[str],
    mapping_method: str,
) -> int:
    """Link regulation to HS codes by 4-digit heading. Returns count."""
    cur = conn.cursor()
    count = 0
    try:
        # Remove old derived mappings for this regulation.
        cur.execute(
            """
            DELETE FROM regulation_hs_codes
            WHERE regulation_id = %s
              AND mapping_method IN ('derived_from_eurlex', 'fallback_curated',
                                     'manual_seed_chapter')
            """,
            (reg_id,),
        )
        for heading in headings:
            cur.execute(
                """
                INSERT INTO regulation_hs_codes
                    (regulation_id, hs_code_id, confidence, mapping_method, reviewed)
                SELECT %s, id, %s, %s, FALSE
                FROM kb_hs_codes
                WHERE code = %s AND code_type = 'EU_CN_4'
                ON CONFLICT DO NOTHING
                """,
                (reg_id, CONFIDENCE, mapping_method, heading),
            )
            count += cur.rowcount
        return count
    finally:
        cur.close()


def load_directives(
    conn,
    use_llm: bool = False,
    dry_run: bool = False,
) -> dict:
    """Fetch directives from CELLAR and seed regulations + HS mappings."""
    now = datetime.now(timezone.utc)
    celex_list = list(NLF_DIRECTIVES.keys())

    logger.info("Fetching %d directives from CELLAR...", len(celex_list))
    fetched = fetch_all_directives(celex_list, delay=1.0)

    llm = None
    if use_llm:
        from pipeline.llm_client import LLMClient
        llm = LLMClient()
        if not llm.health_check():
            logger.warning("Ollama not reachable — falling back to curated mappings")
            llm = None

    reg_count = 0
    hs_count = 0
    skipped: list[tuple[str, str]] = []
    by_directive: dict[str, dict] = {}

    for info in fetched:
        meta = NLF_DIRECTIVES[info.celex]
        dref = meta["directive_ref"]

        if not info.fetch_ok:
            skipped.append((dref, info.error))
            continue

        if dry_run:
            print(f"\n{dref} ({meta['short']}):")
            print(f"  Title: {info.title[:120] if info.title else '(none)'}...")
            print(f"  Scope: {len(info.scope_text)} chars")
            continue

        # 1. Upsert regulation.
        try:
            reg_id = _upsert_regulation(conn, info.celex, meta, info, now)
            reg_count += 1
        except Exception as exc:
            conn.rollback()
            skipped.append((dref, f"db-error: {exc}"))
            continue

        # 2. Derive HS headings.
        headings: list[str] = []
        mapping_method = "fallback_curated"

        if llm and info.scope_text:
            try:
                result = llm.map_directive_to_hs_headings(
                    dref, info.scope_text, info.definitions_text,
                )
                headings = result.get("hs_headings", [])
                mapping_method = "derived_from_eurlex"
                logger.info(
                    "  LLM mapped %s → %d headings (conf %.2f)",
                    dref, len(headings), result.get("confidence", 0),
                )
            except Exception as exc:
                logger.warning("  LLM failed for %s: %s — using fallback", dref, exc)

        if not headings:
            headings = FALLBACK_HS_HEADINGS.get(dref, [])
            mapping_method = "fallback_curated"

        # 3. Link HS headings.
        n = _link_hs_headings(conn, reg_id, headings, mapping_method)
        hs_count += n

        conn.commit()
        by_directive[dref] = {
            "short": meta["short"],
            "reg_id": reg_id,
            "headings_input": len(headings),
            "hs_codes_linked": n,
            "method": mapping_method,
        }

    return {
        "regulations_upserted": reg_count,
        "hs_links_created": hs_count,
        "by_directive": by_directive,
        "skipped": skipped,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ap = argparse.ArgumentParser(description="Load EU NLF directives from CELLAR")
    ap.add_argument("--use-llm", action="store_true", help="Use Ollama for HS mapping")
    ap.add_argument("--dry-run", action="store_true", help="Fetch only, don't write DB")
    args = ap.parse_args()

    if args.dry_run:
        # Dry run doesn't need DB.
        load_directives(None, use_llm=False, dry_run=True)
        return 0

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = load_directives(conn, use_llm=args.use_llm)
    finally:
        conn.close()

    print(f"\nRegulations upserted: {result['regulations_upserted']}")
    print(f"HS links created:     {result['hs_links_created']}")
    print("\nPer directive:")
    for dref, info in sorted(result["by_directive"].items()):
        print(
            f"  {dref:16s} {info['short']:25s} "
            f"headings={info['headings_input']:>3d}  "
            f"linked={info['hs_codes_linked']:>3d}  "
            f"method={info['method']}"
        )
    if result["skipped"]:
        print(f"\nSkipped: {len(result['skipped'])}")
        for name, reason in result["skipped"]:
            print(f"  {name}: {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
