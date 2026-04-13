"""HS code search and compliance check endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_db, get_llm
from api.models import (
    HsCodeSearchRequest,
    HsCodeSearchResponse,
    HsCodeResult,
    ComplianceCheckRequest,
    ComplianceCheckResponse,
    CountryContext,
    RegulationSummary,
)
from pipeline.hs_classifier import (
    get_country_scope,
    get_candidates_by_keyword,
    lookup_mapping,
)

router = APIRouter(prefix="/hs-codes", tags=["hs-codes"])


@router.post("/search", response_model=HsCodeSearchResponse)
def search_hs_codes(
    body: HsCodeSearchRequest,
    conn=Depends(get_db),
    llm=Depends(get_llm),
):
    """Classify a product description into an HS code.

    Uses the two-tier strategy: lookup verified mappings first,
    then GIN full-text search + LLM classification.
    """
    description = body.product_description.strip()
    if not description:
        raise HTTPException(status_code=400, detail="product_description is required")

    country_scope = body.country_scope or "WCO"

    # 1. Try verified lookup
    existing = lookup_mapping(description, country_scope, conn)
    if existing:
        classification = HsCodeResult(
            code=existing["code"],
            code_type=existing.get("code_type"),
            description=description,
            confidence=existing.get("confidence"),
            reasoning=existing.get("reasoning"),
            national_variant=existing.get("national_variant"),
        )
        return HsCodeSearchResponse(classification=classification, candidates=[])

    # 2. GIN full-text search for candidates
    candidate_rows = get_candidates_by_keyword(
        description, country_scope, conn, limit=10,
    )
    candidates = [
        HsCodeResult(
            code=c["code"],
            code_type=c.get("code_type"),
            description=c.get("description"),
        )
        for c in candidate_rows
    ]

    if not candidate_rows:
        classification = HsCodeResult(
            code="",
            description=description,
            confidence=0.0,
            reasoning="No candidates found in HS code library",
        )
        return HsCodeSearchResponse(classification=classification, candidates=[])

    # 3. LLM classification
    llm_result = llm.classify_hs_code(description, candidate_rows)

    classification = HsCodeResult(
        code=llm_result.get("code", ""),
        code_type=llm_result.get("code_type"),
        description=description,
        confidence=llm_result.get("confidence"),
        reasoning=llm_result.get("reasoning"),
        national_variant=llm_result.get("national_variant"),
    )

    return HsCodeSearchResponse(classification=classification, candidates=candidates)


@router.post("/compliance-check", response_model=ComplianceCheckResponse)
def compliance_check(
    body: ComplianceCheckRequest,
    conn=Depends(get_db),
    llm=Depends(get_llm),
):
    """Check compliance for a product in a given country.

    Returns country context, HS classification, matching regulations,
    and applicable standards.
    """
    country_code = body.country_code.strip().upper()
    product_description = body.product_description.strip()

    if not country_code or not product_description:
        raise HTTPException(
            status_code=400,
            detail="country_code and product_description are required",
        )

    cur = conn.cursor()
    try:
        # 1. Country context
        cur.execute(
            """
            SELECT cp.iso2, cp.country_name,
                   eb.code AS block_code, eb.name AS block_name
            FROM kb_country_profiles cp
            LEFT JOIN kb_economic_blocks eb ON cp.block_id = eb.id
            WHERE cp.iso2 = %s
            """,
            (country_code,),
        )
        country_row = cur.fetchone()
        if not country_row:
            raise HTTPException(status_code=404, detail="Country not found")

        cur.execute(
            """
            SELECT org_code FROM kb_memberships
            WHERE country_id = (
                SELECT id FROM kb_country_profiles WHERE iso2 = %s
            ) AND is_member = TRUE
            """,
            (country_code,),
        )
        memberships = [r["org_code"] for r in cur.fetchall()]

        country_ctx = CountryContext(
            iso2=country_row["iso2"],
            country_name=country_row["country_name"],
            block_code=country_row.get("block_code"),
            block_name=country_row.get("block_name"),
            memberships=memberships,
        )

        # 2. HS classification
        country_scope = get_country_scope(country_code, conn)
        existing = lookup_mapping(product_description, country_scope, conn)

        hs_classification = None
        hs_code_value = None

        if existing:
            hs_classification = HsCodeResult(
                code=existing["code"],
                code_type=existing.get("code_type"),
                description=product_description,
                confidence=existing.get("confidence"),
                reasoning=existing.get("reasoning"),
                national_variant=existing.get("national_variant"),
            )
            hs_code_value = existing["code"]
        else:
            candidate_rows = get_candidates_by_keyword(
                product_description, country_scope, conn, limit=10,
            )
            if candidate_rows:
                llm_result = llm.classify_hs_code(
                    product_description, candidate_rows,
                )
                hs_classification = HsCodeResult(
                    code=llm_result.get("code", ""),
                    code_type=llm_result.get("code_type"),
                    description=product_description,
                    confidence=llm_result.get("confidence"),
                    reasoning=llm_result.get("reasoning"),
                    national_variant=llm_result.get("national_variant"),
                )
                hs_code_value = llm_result.get("code")

        # 3. Matching regulations
        reg_conditions = ["r.country = %s"]
        reg_params: list = [country_code]

        if hs_code_value:
            cur.execute(
                """
                SELECT r.id, r.title, r.document_type, r.authority,
                       r.country, r.effective_date, r.status
                FROM regulations r
                JOIN regulation_hs_codes rhc ON r.id = rhc.regulation_id
                JOIN kb_hs_codes hc ON rhc.hs_code_id = hc.id
                WHERE r.country = %s AND hc.code = %s
                ORDER BY r.effective_date DESC NULLS LAST
                LIMIT 20
                """,
                (country_code, hs_code_value),
            )
        else:
            cur.execute(
                """
                SELECT r.id, r.title, r.document_type, r.authority,
                       r.country, r.effective_date, r.status
                FROM regulations r
                WHERE r.country = %s
                ORDER BY r.effective_date DESC NULLS LAST
                LIMIT 20
                """,
                (country_code,),
            )
        reg_rows = cur.fetchall()

        # 4. Standards
        cur.execute(
            """
            SELECT DISTINCT standard_name
            FROM kb_standards
            WHERE country_code = %s OR country_code = 'INT'
            ORDER BY standard_name
            LIMIT 50
            """,
            (country_code,),
        )
        standards = [r["standard_name"] for r in cur.fetchall()]

    finally:
        cur.close()

    regulations = [RegulationSummary(**r) for r in reg_rows]

    return ComplianceCheckResponse(
        country_context=country_ctx,
        hs_classification=hs_classification,
        regulations=regulations,
        standards=standards,
    )
