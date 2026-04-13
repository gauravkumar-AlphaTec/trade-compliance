"""LLM-driven extraction functions for KB country profiles.

All functions accept an LLMClient instance — never call Ollama directly.
Every field carries source_url, confidence, and last_verified_at per KB rules.
"""

import json
import logging
from datetime import datetime, timezone

from pipeline.llm_client import LLMClient

logger = logging.getLogger(__name__)

# Per CLAUDE.md: free-form text fields always default to confidence=0.65
INSIGHTS_CONFIDENCE = 0.65


# ------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------

QIB_SCHEMA = {
    "national_standards_body": {
        "name": "string",
        "acronym": "string or null",
        "url": "string or null",
        "scope": "string or null",
    },
    "accreditation_body": {
        "name": "string",
        "acronym": "string or null",
        "url": "string or null",
        "scope": "string or null",
    },
    "metrology_institute": {
        "name": "string",
        "acronym": "string or null",
        "url": "string or null",
        "scope": "string or null",
    },
    "legal_metrology_body": {
        "name": "string or null",
        "acronym": "string or null",
        "url": "string or null",
        "scope": "string or null",
    },
}

LEGAL_FRAMEWORK_SCHEMA = {
    "laws": [
        {
            "title": "string",
            "law_type": "national_law | regulation | decree",
            "scope": "string or null",
            "url": "string or null",
            "standards_mandatory": "boolean",
            "local_adaptation_notes": "string or null",
        }
    ],
    "regulatory_authorities": [
        {
            "name": "string",
            "acronym": "string or null",
            "scope": "string or null",
            "url": "string or null",
            "authority_type": "customs | market_surveillance | standards | sectoral",
        }
    ],
}

EU_DEVIATIONS_SCHEMA = {
    "deviations": [
        {
            "reference_standard": "string",
            "deviation_type": "scope | method | thresholds | additional_requirements",
            "description": "string",
            "documentation_required": ["string"],
        }
    ],
    "standards_acceptance": [
        {
            "standard_code": "string",
            "standard_name": "string or null",
            "standard_type": "design | testing | labeling | process",
            "accepted": "boolean",
            "national_equivalent": "string or null",
            "harmonization_level": "full | partial | none | pending",
            "comments": "string or null",
        }
    ],
}

NON_EU_STANDARDS_SCHEMA = {
    "standards_acceptance": [
        {
            "standard_code": "string",
            "standard_name": "string or null",
            "standard_type": "design | testing | labeling | process",
            "accepted": "boolean",
            "national_equivalent": "string or null",
            "harmonization_level": "full | partial | none | pending",
            "comments": "string or null",
        }
    ],
}

TESTING_PROTOCOLS_SCHEMA = {
    "protocols": [
        {
            "protocol_name": "string",
            "accepted": "boolean",
            "accepted_conditionally": "boolean",
            "conditions": [{"type": "string", "description": "string"}],
            "notes": "string or null",
        }
    ],
}

INSIGHTS_SCHEMA = {
    "local_challenges": "string or null",
    "recent_reforms": "string or null",
    "useful_portals": [{"name": "string", "url": "string", "description": "string"}],
    "regulatory_deadlines": [{"description": "string", "date": "string"}],
    "general_notes": "string or null",
}


# ------------------------------------------------------------------
# Extraction functions
# ------------------------------------------------------------------

def extract_qib(
    country_name: str,
    iso2: str,
    source_url: str,
    page_content: str,
    llm: LLMClient,
) -> dict:
    """Extract quality infrastructure bodies from page content.

    Returns dict with keys: national_standards_body, accreditation_body,
    metrology_institute, legal_metrology_body.  Each value is a JSONB-ready
    dict with embedded provenance (source_url, confidence, last_verified_at).
    """
    now = datetime.now(timezone.utc).isoformat()
    system_context = (
        f"You are a trade-compliance research assistant extracting quality "
        f"infrastructure body information for {country_name} ({iso2}). "
        f"Identify the national standards body, accreditation body, "
        f"metrology institute, and legal metrology body. "
        f"Use only information present in the provided text."
    )

    raw = llm.extract_structured(page_content, QIB_SCHEMA, system_context)

    # Wrap each body with provenance metadata
    result = {}
    for key in ("national_standards_body", "accreditation_body",
                "metrology_institute", "legal_metrology_body"):
        value = raw.get(key)
        result[key] = {
            "value": value,
            "source_url": source_url,
            "confidence": 0.90,
            "last_verified_at": now,
        }

    return result


def extract_legal_framework(
    country_name: str,
    iso2: str,
    page_content: str,
    llm: LLMClient,
) -> list[dict]:
    """Extract laws and regulatory authorities from page content.

    Returns list of dicts matching kb_laws and kb_regulatory_authorities schema.
    """
    now = datetime.now(timezone.utc).isoformat()
    system_context = (
        f"You are a trade-compliance research assistant extracting the legal "
        f"and regulatory framework for {country_name} ({iso2}). "
        f"Identify all relevant laws, regulations, decrees, and regulatory "
        f"authorities related to product conformity, market access, and "
        f"technical standards. Use only information present in the provided text."
    )

    raw = llm.extract_structured(page_content, LEGAL_FRAMEWORK_SCHEMA, system_context)

    results = []
    for law in raw.get("laws", []):
        law["confidence"] = 0.80
        law["last_verified_at"] = now
        law["record_type"] = "law"
        results.append(law)

    for auth in raw.get("regulatory_authorities", []):
        auth["confidence"] = 0.80
        auth["last_verified_at"] = now
        auth["record_type"] = "regulatory_authority"
        results.append(auth)

    return results


def extract_standards_deviations(
    country_name: str,
    iso2: str,
    is_eu_member: bool,
    product_categories: list[str],
    page_content: str,
    llm: LLMClient,
) -> list[dict]:
    """Extract standards acceptance and national deviations.

    For EU members: extracts deviations from harmonized EU standards.
    For non-EU countries: extracts full standards acceptance information.
    """
    now = datetime.now(timezone.utc).isoformat()
    categories_str = ", ".join(product_categories) if product_categories else "general"

    if is_eu_member:
        system_context = (
            f"You are a trade-compliance research assistant analysing national "
            f"deviations from EU harmonized standards for {country_name} ({iso2}). "
            f"Focus on product categories: {categories_str}. "
            f"Identify any national deviations, additional requirements, or "
            f"differences from the EU harmonized standards. Also list any "
            f"standards with country-specific acceptance status. "
            f"Use only information present in the provided text."
        )
        schema = EU_DEVIATIONS_SCHEMA
    else:
        system_context = (
            f"You are a trade-compliance research assistant analysing standards "
            f"acceptance for {country_name} ({iso2}). "
            f"Focus on product categories: {categories_str}. "
            f"Identify which international standards (ISO, IEC, etc.) are "
            f"accepted, whether national equivalents exist, the level of "
            f"harmonization, and any conditions or restrictions. "
            f"Use only information present in the provided text."
        )
        schema = NON_EU_STANDARDS_SCHEMA

    raw = llm.extract_structured(page_content, schema, system_context)

    results = []
    for deviation in raw.get("deviations", []):
        deviation["confidence"] = 0.80
        deviation["last_verified_at"] = now
        deviation["record_type"] = "deviation"
        results.append(deviation)

    for std in raw.get("standards_acceptance", []):
        std["confidence"] = 0.80
        std["last_verified_at"] = now
        std["record_type"] = "standards_acceptance"
        results.append(std)

    return results


def extract_testing_protocols(
    country_name: str,
    iso2: str,
    page_content: str,
    llm: LLMClient,
) -> list[dict]:
    """Extract testing protocol acceptance from page content.

    Covers CB Scheme, IECEx, IECEE, and other international testing
    mutual recognition arrangements.
    """
    now = datetime.now(timezone.utc).isoformat()
    system_context = (
        f"You are a trade-compliance research assistant extracting testing "
        f"protocol acceptance information for {country_name} ({iso2}). "
        f"Identify acceptance of international testing protocols including "
        f"CB Scheme (IECEE), IECEx certificates, and any other mutual "
        f"recognition arrangements for test reports. Note whether acceptance "
        f"is full, conditional, or not accepted, and list any conditions. "
        f"Use only information present in the provided text."
    )

    raw = llm.extract_structured(page_content, TESTING_PROTOCOLS_SCHEMA, system_context)

    results = []
    for protocol in raw.get("protocols", []):
        protocol["confidence"] = 0.80
        protocol["last_verified_at"] = now
        results.append(protocol)

    return results


def extract_insights(
    country_name: str,
    iso2: str,
    llm: LLMClient,
) -> dict:
    """Generate practical compliance insights using Opus's own knowledge.

    Does NOT read from a web page — relies on the model's training data.
    Per CLAUDE.md: free-form text fields always default to confidence=0.65.
    """
    now = datetime.now(timezone.utc).isoformat()
    system_context = (
        f"You are a trade-compliance expert with deep knowledge of "
        f"{country_name} ({iso2}). Provide practical insights for companies "
        f"trying to export products to this country. Draw on your knowledge "
        f"of the regulatory environment, common challenges, recent reforms, "
        f"and useful government portals."
    )

    # No page_content — use a prompt-only extraction
    prompt_text = (
        f"Based on your knowledge, provide practical trade compliance "
        f"insights for {country_name} ({iso2}). Cover:\n"
        f"- Local challenges exporters commonly face\n"
        f"- Recent regulatory reforms or changes\n"
        f"- Useful government portals and websites (with URLs)\n"
        f"- Upcoming regulatory deadlines (if any)\n"
        f"- General notes and tips for compliance"
    )

    raw = llm.extract_structured(prompt_text, INSIGHTS_SCHEMA, system_context)

    # Hard-code confidence=0.65 per CLAUDE.md KB design rules
    raw["confidence"] = INSIGHTS_CONFIDENCE
    raw["last_verified_at"] = now

    return raw
