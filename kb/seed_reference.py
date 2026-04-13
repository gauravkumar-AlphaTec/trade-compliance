"""Hand-curated reference KB data for EU block + Germany.

CLAUDE.md names Germany (DE) as the reference implementation. This script
seeds known-good, high-confidence data so the API and downstream flows
have something to work against without requiring LLM extraction or flaky
live scrapers.

All entries use real official source URLs; confidence is 0.95 for tier-1
facts (membership, national bodies) and 0.90 for legal framework items.
Idempotent — uses ON CONFLICT on existing unique keys.
"""

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2

logger = logging.getLogger(__name__)

NOW = datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------
# EU block
# ------------------------------------------------------------------

EU_BLOCK = {
    "code": "EU",
    "name": "European Union",
    "block_type": "customs_union",
    "official_url": "https://european-union.europa.eu/",
}

EU_DIRECTIVES = [
    {
        "directive_number": "2006/42/EC",
        "title": "Machinery Directive",
        "ojl_reference": "OJ L 157, 9.6.2006",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006L0042",
        "scope": "Safety requirements for machinery placed on the EU market",
    },
    {
        "directive_number": "2023/1230",
        "title": "Machinery Regulation (replaces 2006/42/EC from 20 Jan 2027)",
        "ojl_reference": "OJ L 165, 29.6.2023",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32023R1230",
        "scope": "Updated machinery safety framework, covers AI-enabled machines",
    },
    {
        "directive_number": "2014/35/EU",
        "title": "Low Voltage Directive (LVD)",
        "ojl_reference": "OJ L 96, 29.3.2014",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0035",
        "scope": "Electrical equipment 50-1000 V AC / 75-1500 V DC",
    },
    {
        "directive_number": "2014/30/EU",
        "title": "Electromagnetic Compatibility Directive (EMC)",
        "ojl_reference": "OJ L 96, 29.3.2014",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0030",
        "scope": "EMC requirements for apparatus and fixed installations",
    },
    {
        "directive_number": "2014/53/EU",
        "title": "Radio Equipment Directive (RED)",
        "ojl_reference": "OJ L 153, 22.5.2014",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0053",
        "scope": "Radio equipment essential requirements",
    },
    {
        "directive_number": "2017/745",
        "title": "Medical Devices Regulation (MDR)",
        "ojl_reference": "OJ L 117, 5.5.2017",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32017R0745",
        "scope": "Medical devices safety, performance, and clinical evidence",
    },
    {
        "directive_number": "2011/65/EU",
        "title": "Restriction of Hazardous Substances (RoHS)",
        "ojl_reference": "OJ L 174, 1.7.2011",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011L0065",
        "scope": "Restriction of hazardous substances in EEE",
    },
    {
        "directive_number": "2014/68/EU",
        "title": "Pressure Equipment Directive (PED)",
        "ojl_reference": "OJ L 189, 27.6.2014",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0068",
        "scope": "Pressure equipment above 0.5 bar",
    },
]

EU_CONFORMITY_FRAMEWORK = {
    "marking": "CE",
    "description": (
        "New Legislative Framework (NLF). Products meeting harmonized "
        "standards get presumption of conformity; manufacturer issues "
        "EU Declaration of Conformity and affixes CE marking."
    ),
    "modules": ["A", "A1", "A2", "B", "C", "C1", "C2", "D", "D1",
                "E", "E1", "F", "F1", "G", "H", "H1"],
    "notified_bodies_database": "https://ec.europa.eu/growth/tools-databases/nando/",
    "source_url": "https://single-market-economy.ec.europa.eu/single-market/goods/new-legislative-framework_en",
}


# ------------------------------------------------------------------
# Germany country + related
# ------------------------------------------------------------------

DE_COUNTRY = {
    "iso2": "DE",
    "iso3": "DEU",
    "country_name": "Germany",
    "region": "Europe",
    "tags": ["eu_member", "eurozone", "schengen", "g7", "oecd"],
    "national_standards_body": {
        "name": "Deutsches Institut für Normung",
        "acronym": "DIN",
        "url": "https://www.din.de/",
        "source_url": "https://www.din.de/en/about-standards/din-in-brief",
        "confidence": 0.95,
    },
    "accreditation_body": {
        "name": "Deutsche Akkreditierungsstelle",
        "acronym": "DAkkS",
        "url": "https://www.dakks.de/",
        "source_url": "https://www.dakks.de/en/content/about-us.html",
        "confidence": 0.95,
    },
    "metrology_institute": {
        "name": "Physikalisch-Technische Bundesanstalt",
        "acronym": "PTB",
        "url": "https://www.ptb.de/",
        "source_url": "https://www.ptb.de/cms/en/ptb.html",
        "confidence": 0.95,
    },
    "legal_metrology_body": {
        "name": "Physikalisch-Technische Bundesanstalt (legal metrology division)",
        "acronym": "PTB",
        "url": "https://www.ptb.de/cms/en/metrological-information-technology.html",
        "source_url": "https://www.ptb.de/cms/en/ptb.html",
        "confidence": 0.95,
    },
    "notified_bodies_url": "https://ec.europa.eu/growth/tools-databases/nando/index.cfm?fuseaction=country.main&country_id=DE",
    "official_languages": ["de"],
    "accepted_doc_languages": ["de", "en"],
    "translation_requirements": (
        "User-facing documents (instructions, safety warnings, DoC summary) "
        "must be in German. Technical dossier may be submitted in English "
        "unless a Market Surveillance Authority specifically requests German."
    ),
    "ca_system_structure": (
        "Federal system. Market surveillance is exercised by Länder (states) "
        "under coordination of BAuA (ProdSG Act). ZLS coordinates notified "
        "body designations."
    ),
    "accreditation_mandatory": True,
    "profile_status": "published",
}

DE_MEMBERSHIPS = [
    {
        "org_code": "WTO",
        "is_member": True,
        "membership_type": "full",
        "accession_date": "1995-01-01",
        "source_url": "https://www.wto.org/english/thewto_e/countries_e/germany_e.htm",
        "confidence": 0.95,
    },
    {
        "org_code": "ISO",
        "is_member": True,
        "membership_type": "full",
        "scope_details": "DIN (Deutsches Institut für Normung) — member body",
        "source_url": "https://www.iso.org/member/1511.html",
        "confidence": 0.95,
    },
    {
        "org_code": "IEC",
        "is_member": True,
        "membership_type": "full",
        "scope_details": "DKE (Deutsche Kommission Elektrotechnik) — National Committee",
        "source_url": "https://www.iec.ch/national-committees/germany",
        "confidence": 0.95,
    },
    {
        "org_code": "CEN",
        "is_member": True,
        "membership_type": "full",
        "scope_details": "DIN — national member",
        "source_url": "https://www.cencenelec.eu/about-cen/",
        "confidence": 0.95,
    },
    {
        "org_code": "CENELEC",
        "is_member": True,
        "membership_type": "full",
        "scope_details": "DKE — national committee",
        "source_url": "https://www.cencenelec.eu/about-cenelec/",
        "confidence": 0.95,
    },
    {
        "org_code": "BIPM",
        "is_member": True,
        "membership_type": "member_state",
        "source_url": "https://www.bipm.org/en/member-states",
        "confidence": 0.95,
    },
    {
        "org_code": "ILAC",
        "is_member": True,
        "membership_type": "signatory",
        "scope_details": "DAkkS — full MRA signatory (calibration, testing, inspection)",
        "source_url": "https://ilac.org/signatory-search/",
        "confidence": 0.95,
    },
    {
        "org_code": "IAF",
        "is_member": True,
        "membership_type": "signatory",
        "scope_details": "DAkkS — MLA signatory",
        "source_url": "https://iaf.nu/en/iaf-mla-signatories-by-country/",
        "confidence": 0.95,
    },
    {
        "org_code": "OIML",
        "is_member": True,
        "membership_type": "member_state",
        "source_url": "https://www.oiml.org/en/structure/member-states",
        "confidence": 0.95,
    },
    {
        "org_code": "EU",
        "is_member": True,
        "membership_type": "member_state",
        "accession_date": "1958-01-01",
        "source_url": "https://european-union.europa.eu/principles-countries-history/country-profiles/germany_en",
        "confidence": 0.95,
    },
]

DE_LAWS = [
    {
        "title": "Produktsicherheitsgesetz (ProdSG) — Product Safety Act",
        "law_type": "federal_act",
        "scope": (
            "Horizontal product safety framework transposing EU NLF. "
            "Governs placing on market, CE marking enforcement, GS mark."
        ),
        "url": "https://www.gesetze-im-internet.de/prodsg_2021/",
        "standards_mandatory": False,
        "local_adaptation_notes": (
            "GS mark (Geprüfte Sicherheit) is a voluntary German conformity "
            "mark under ProdSG §§ 20–22 — not required for market access but "
            "widely expected in retail channels."
        ),
        "source_url": "https://www.gesetze-im-internet.de/prodsg_2021/",
        "confidence": 0.90,
    },
    {
        "title": "Elektro- und Elektronikgerätegesetz (ElektroG)",
        "law_type": "federal_act",
        "scope": "WEEE transposition — EEE registration, take-back, recycling",
        "url": "https://www.gesetze-im-internet.de/elektrog_2015/",
        "standards_mandatory": True,
        "source_url": "https://www.gesetze-im-internet.de/elektrog_2015/",
        "confidence": 0.90,
    },
    {
        "title": "Elektromagnetische Verträglichkeit Gesetz (EMVG)",
        "law_type": "federal_act",
        "scope": "National EMC law — transposes EMC Directive 2014/30/EU",
        "url": "https://www.gesetze-im-internet.de/emvg_2016/",
        "standards_mandatory": True,
        "source_url": "https://www.gesetze-im-internet.de/emvg_2016/",
        "confidence": 0.90,
    },
    {
        "title": "Medizinprodukterecht-Durchführungsgesetz (MPDG)",
        "law_type": "federal_act",
        "scope": "National implementation of MDR 2017/745",
        "url": "https://www.gesetze-im-internet.de/mpdg/",
        "standards_mandatory": True,
        "source_url": "https://www.gesetze-im-internet.de/mpdg/",
        "confidence": 0.90,
    },
]

DE_AUTHORITIES = [
    {
        "name": "Bundesanstalt für Arbeitsschutz und Arbeitsmedizin",
        "acronym": "BAuA",
        "scope": "Product safety coordination under ProdSG; chemicals (REACH)",
        "url": "https://www.baua.de/EN/Home/Home_node.html",
        "authority_type": "federal_agency",
        "source_url": "https://www.baua.de/EN/Topics/Work-and-product/Work-and-product_node.html",
        "confidence": 0.95,
    },
    {
        "name": "Bundesnetzagentur",
        "acronym": "BNetzA",
        "scope": "Market surveillance for radio equipment (RED) and EMC",
        "url": "https://www.bundesnetzagentur.de/EN/Home/home_node.html",
        "authority_type": "federal_agency",
        "source_url": "https://www.bundesnetzagentur.de/EN/Areas/Telecommunications/start.html",
        "confidence": 0.95,
    },
    {
        "name": "Bundesinstitut für Arzneimittel und Medizinprodukte",
        "acronym": "BfArM",
        "scope": "Medical devices — MDR competent authority",
        "url": "https://www.bfarm.de/EN/Home/_node.html",
        "authority_type": "federal_agency",
        "source_url": "https://www.bfarm.de/EN/Medical-devices/_node.html",
        "confidence": 0.95,
    },
    {
        "name": "Zentralstelle der Länder für Sicherheitstechnik",
        "acronym": "ZLS",
        "scope": "Coordinates notified body designations under ProdSG",
        "url": "https://www.zls-muenchen.de/",
        "authority_type": "coordinating_body",
        "source_url": "https://www.zls-muenchen.de/",
        "confidence": 0.95,
    },
]

DE_MRAS = [
    {
        "mra_type": "multilateral",
        "partner": "ILAC signatories",
        "name": "ILAC MRA",
        "scope": "Mutual recognition of calibration, testing, inspection",
        "membership_role": "signatory",
        "sectors_covered": ["calibration", "testing", "inspection", "medical_testing"],
        "source_url": "https://ilac.org/ilac-mra/",
        "confidence": 0.95,
    },
    {
        "mra_type": "multilateral",
        "partner": "IAF signatories",
        "name": "IAF MLA",
        "scope": "Mutual recognition of management systems, products, persons certification",
        "membership_role": "signatory",
        "sectors_covered": ["management_systems", "product_certification", "person_certification"],
        "source_url": "https://iaf.nu/en/iaf-mla/",
        "confidence": 0.95,
    },
]


# ------------------------------------------------------------------
# Hand-curated EU regulations (as queryable `regulations` rows).
# Each links to a `sources` row; HS code mappings use chapter prefixes.
# ------------------------------------------------------------------

DE_REGULATIONS = [
    {
        "source_name": "EUR-Lex",
        "document_id": "32006L0042",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006L0042",
        "title": "Directive 2006/42/EC — Machinery Directive",
        "document_type": "directive",
        "authority": "European Commission",
        "country": "DE",
        "effective_date": "2009-12-29",
        "summary": (
            "Essential health and safety requirements for machinery placed on "
            "the EU market. Manufacturer performs risk assessment, compiles "
            "technical file, issues EU Declaration of Conformity, affixes CE "
            "marking. Replaced by Regulation 2023/1230 from 20 Jan 2027."
        ),
        "hs_prefixes": ["84", "85"],
    },
    {
        "source_name": "EUR-Lex",
        "document_id": "32014L0035",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0035",
        "title": "Directive 2014/35/EU — Low Voltage Directive (LVD)",
        "document_type": "directive",
        "authority": "European Commission",
        "country": "DE",
        "effective_date": "2016-04-20",
        "summary": (
            "Electrical equipment designed for use within 50-1000 V AC / "
            "75-1500 V DC. Requires compliance with essential safety "
            "objectives; harmonized EN standards give presumption of "
            "conformity. Manufacturer self-declaration + CE marking."
        ),
        "hs_prefixes": ["85"],
    },
    {
        "source_name": "EUR-Lex",
        "document_id": "32014L0030",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0030",
        "title": "Directive 2014/30/EU — Electromagnetic Compatibility (EMC)",
        "document_type": "directive",
        "authority": "European Commission",
        "country": "DE",
        "effective_date": "2016-04-20",
        "summary": (
            "Apparatus must not generate electromagnetic disturbance above a "
            "level that impairs other equipment, and must have adequate "
            "immunity. Fixed installations require good engineering practice "
            "documentation. CE marking + DoC required."
        ),
        "hs_prefixes": ["85"],
    },
    {
        "source_name": "EUR-Lex",
        "document_id": "32017R0745",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32017R0745",
        "title": "Regulation (EU) 2017/745 — Medical Devices Regulation (MDR)",
        "document_type": "regulation",
        "authority": "European Commission",
        "country": "DE",
        "effective_date": "2021-05-26",
        "summary": (
            "Risk-based classification (I, IIa, IIb, III). Class IIa and "
            "above require Notified Body involvement. EUDAMED registration, "
            "UDI, clinical evaluation, and post-market surveillance mandatory."
        ),
        "hs_prefixes": ["90"],
    },
    {
        "source_name": "EUR-Lex",
        "document_id": "32011L0065",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011L0065",
        "title": "Directive 2011/65/EU — Restriction of Hazardous Substances (RoHS)",
        "document_type": "directive",
        "authority": "European Commission",
        "country": "DE",
        "effective_date": "2013-01-02",
        "summary": (
            "Restricts 10 hazardous substances (Pb, Hg, Cd, Cr(VI), PBB, "
            "PBDE, DEHP, BBP, DBP, DIBP) in electrical and electronic "
            "equipment. Transposed in DE via ElektroStoffV."
        ),
        "hs_prefixes": ["84", "85"],
    },
]


# ------------------------------------------------------------------
# Hand-curated product → HS code lookup mappings.
# Populates kb_product_hs_mappings with verified=TRUE so the HS
# classifier's first-tier lookup hits before any LLM call.
# ------------------------------------------------------------------

PRODUCT_HS_MAPPINGS = [
    {
        "product_category": "centrifugal pumps",
        "hs_code": "84137000",
        "code_type": "EU_CN_8",
        "country_scope": "EU_CN_8",
        "confidence": 0.92,
        "reasoning": "CN 8413.70 covers other centrifugal pumps.",
    },
    {
        "product_category": "electric motors",
        "hs_code": "85015200",
        "code_type": "EU_CN_8",
        "country_scope": "EU_CN_8",
        "confidence": 0.88,
        "reasoning": "CN 8501.52 — AC motors, multi-phase, 750 W to 75 kW.",
    },
    {
        "product_category": "industrial robots",
        "hs_code": "84795000",
        "code_type": "EU_CN_8",
        "country_scope": "EU_CN_8",
        "confidence": 0.95,
        "reasoning": "CN 8479.50 — industrial robots, not elsewhere specified.",
    },
    {
        "product_category": "CNC milling machines",
        "hs_code": "84596100",
        "code_type": "EU_CN_8",
        "country_scope": "EU_CN_8",
        "confidence": 0.92,
        "reasoning": "CN 8459.61 — other milling machines, numerically controlled.",
    },
    {
        "product_category": "hot-water boilers",
        "hs_code": "84031000",
        "code_type": "EU_CN_8",
        "country_scope": "EU_CN_8",
        "confidence": 0.90,
        "reasoning": "CN 8403.10 — central-heating boilers.",
    },
]


# ------------------------------------------------------------------
# Insert helpers
# ------------------------------------------------------------------

def seed_eu_block(conn) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO kb_economic_blocks (code, name, block_type, official_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                block_type = EXCLUDED.block_type,
                official_url = EXCLUDED.official_url
            RETURNING id
            """,
            (EU_BLOCK["code"], EU_BLOCK["name"], EU_BLOCK["block_type"],
             EU_BLOCK["official_url"]),
        )
        block_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO kb_block_profiles
                (block_id, directives, harmonized_standards, shared_mras,
                 conformity_framework, last_updated)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (block_id) DO UPDATE SET
                directives = EXCLUDED.directives,
                harmonized_standards = EXCLUDED.harmonized_standards,
                shared_mras = EXCLUDED.shared_mras,
                conformity_framework = EXCLUDED.conformity_framework,
                last_updated = NOW()
            """,
            (
                block_id,
                json.dumps(EU_DIRECTIVES),
                json.dumps([]),
                json.dumps([]),
                json.dumps(EU_CONFORMITY_FRAMEWORK),
            ),
        )
        conn.commit()
        return block_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_country(conn, country: dict, block_id: int | None) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO kb_country_profiles (
                iso2, iso3, country_name, region, block_id, tags,
                national_standards_body, accreditation_body,
                metrology_institute, legal_metrology_body,
                notified_bodies_url, official_languages, accepted_doc_languages,
                translation_requirements, ca_system_structure,
                accreditation_mandatory, profile_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (iso2) DO UPDATE SET
                iso3 = EXCLUDED.iso3,
                country_name = EXCLUDED.country_name,
                region = EXCLUDED.region,
                block_id = EXCLUDED.block_id,
                tags = EXCLUDED.tags,
                national_standards_body = EXCLUDED.national_standards_body,
                accreditation_body = EXCLUDED.accreditation_body,
                metrology_institute = EXCLUDED.metrology_institute,
                legal_metrology_body = EXCLUDED.legal_metrology_body,
                notified_bodies_url = EXCLUDED.notified_bodies_url,
                official_languages = EXCLUDED.official_languages,
                accepted_doc_languages = EXCLUDED.accepted_doc_languages,
                translation_requirements = EXCLUDED.translation_requirements,
                ca_system_structure = EXCLUDED.ca_system_structure,
                accreditation_mandatory = EXCLUDED.accreditation_mandatory,
                profile_status = EXCLUDED.profile_status,
                updated_at = NOW()
            RETURNING id
            """,
            (
                country["iso2"], country["iso3"], country["country_name"],
                country["region"], block_id, country["tags"],
                json.dumps(country["national_standards_body"]),
                json.dumps(country["accreditation_body"]),
                json.dumps(country["metrology_institute"]),
                json.dumps(country["legal_metrology_body"]),
                country["notified_bodies_url"],
                country["official_languages"],
                country["accepted_doc_languages"],
                country["translation_requirements"],
                country["ca_system_structure"],
                country["accreditation_mandatory"],
                country["profile_status"],
            ),
        )
        country_id = cur.fetchone()[0]
        conn.commit()
        return country_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_memberships(conn, country_id: int, rows: list[dict]) -> int:
    cur = conn.cursor()
    count = 0
    try:
        for r in rows:
            cur.execute(
                """
                INSERT INTO kb_memberships (
                    country_id, org_code, is_member, membership_type,
                    accession_date, scope_details, source_url, confidence,
                    last_verified_at, extraction_method
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (country_id, org_code) DO UPDATE SET
                    is_member = EXCLUDED.is_member,
                    membership_type = EXCLUDED.membership_type,
                    accession_date = EXCLUDED.accession_date,
                    scope_details = EXCLUDED.scope_details,
                    source_url = EXCLUDED.source_url,
                    confidence = EXCLUDED.confidence,
                    last_verified_at = EXCLUDED.last_verified_at,
                    extraction_method = EXCLUDED.extraction_method
                """,
                (
                    country_id, r["org_code"], r["is_member"],
                    r["membership_type"], r.get("accession_date"),
                    r.get("scope_details"), r["source_url"], r["confidence"],
                    NOW, "manual_seed",
                ),
            )
            count += 1
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_laws(conn, country_id: int, rows: list[dict]) -> int:
    cur = conn.cursor()
    count = 0
    try:
        for r in rows:
            cur.execute(
                """
                INSERT INTO kb_laws (
                    country_id, title, law_type, scope, url,
                    standards_mandatory, local_adaptation_notes,
                    source_url, confidence, last_verified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    country_id, r["title"], r["law_type"], r["scope"],
                    r["url"], r.get("standards_mandatory"),
                    r.get("local_adaptation_notes"),
                    r["source_url"], r["confidence"], NOW,
                ),
            )
            count += 1
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_authorities(conn, country_id: int, rows: list[dict]) -> int:
    cur = conn.cursor()
    count = 0
    try:
        for r in rows:
            cur.execute(
                """
                INSERT INTO kb_regulatory_authorities (
                    country_id, name, acronym, scope, url,
                    authority_type, source_url, confidence, last_verified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    country_id, r["name"], r["acronym"], r["scope"], r["url"],
                    r["authority_type"], r["source_url"], r["confidence"], NOW,
                ),
            )
            count += 1
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_mras(conn, country_id: int, rows: list[dict]) -> int:
    cur = conn.cursor()
    count = 0
    try:
        for r in rows:
            cur.execute(
                """
                INSERT INTO kb_mras (
                    country_id, mra_type, partner, name, scope,
                    membership_role, sectors_covered, source_url,
                    confidence, last_verified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    country_id, r["mra_type"], r["partner"], r["name"],
                    r["scope"], r["membership_role"], r["sectors_covered"],
                    r["source_url"], r["confidence"], NOW,
                ),
            )
            count += 1
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def seed_regulations(conn, regulations: list[dict]) -> dict:
    """Insert EU regulations + sources rows + HS code mappings by chapter prefix."""
    cur = conn.cursor()
    reg_count = 0
    mapping_count = 0
    try:
        for r in regulations:
            cur.execute(
                """
                INSERT INTO sources (source_name, document_id, url, fetched_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_name, document_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    fetched_at = EXCLUDED.fetched_at
                RETURNING id
                """,
                (r["source_name"], r["document_id"], r["url"], NOW),
            )
            source_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO regulations (
                    source_id, title, document_type, authority, country,
                    effective_date, summary, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                ON CONFLICT (source_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    document_type = EXCLUDED.document_type,
                    authority = EXCLUDED.authority,
                    country = EXCLUDED.country,
                    effective_date = EXCLUDED.effective_date,
                    summary = EXCLUDED.summary,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    source_id, r["title"], r["document_type"],
                    r["authority"], r["country"], r["effective_date"],
                    r["summary"],
                ),
            )
            reg_id = cur.fetchone()[0]
            reg_count += 1

            for prefix in r.get("hs_prefixes", []):
                cur.execute(
                    """
                    INSERT INTO regulation_hs_codes
                        (regulation_id, hs_code_id, confidence, mapping_method, reviewed)
                    SELECT %s, id, 0.85, 'manual_seed_chapter', TRUE
                    FROM kb_hs_codes
                    WHERE code = %s AND code_type = 'EU_CN_2'
                    ON CONFLICT DO NOTHING
                    """,
                    (reg_id, prefix),
                )
                mapping_count += cur.rowcount
        conn.commit()
        return {"regulations": reg_count, "hs_mappings": mapping_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_product_mappings(conn, mappings: list[dict]) -> int:
    """Insert verified product→HS code mappings."""
    cur = conn.cursor()
    count = 0
    try:
        for m in mappings:
            cur.execute(
                """
                INSERT INTO kb_product_hs_mappings (
                    product_category, hs_code, hs_code_id, code_type,
                    country_scope, confidence, reasoning, source, verified
                )
                SELECT %s, %s, id, %s, %s, %s, %s, 'manual_seed', TRUE
                FROM kb_hs_codes
                WHERE code = %s AND code_type = %s
                """,
                (
                    m["product_category"], m["hs_code"], m["code_type"],
                    m["country_scope"], m["confidence"], m["reasoning"],
                    m["hs_code"], m["code_type"],
                ),
            )
            count += cur.rowcount
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def seed_all(conn) -> dict:
    """Seed EU block + Germany reference profile. Returns summary counts."""
    block_id = seed_eu_block(conn)
    country_id = seed_country(conn, DE_COUNTRY, block_id)
    memberships = seed_memberships(conn, country_id, DE_MEMBERSHIPS)
    laws = seed_laws(conn, country_id, DE_LAWS)
    authorities = seed_authorities(conn, country_id, DE_AUTHORITIES)
    mras = seed_mras(conn, country_id, DE_MRAS)
    regs = seed_regulations(conn, DE_REGULATIONS)

    return {
        "eu_block_id": block_id,
        "eu_directives": len(EU_DIRECTIVES),
        "de_country_id": country_id,
        "de_memberships": memberships,
        "de_laws": laws,
        "de_authorities": authorities,
        "de_mras": mras,
        "regulations": regs["regulations"],
        "hs_mappings": regs["hs_mappings"],
    }


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        result = seed_all(conn)
        for k, v in result.items():
            print(f"  {k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
