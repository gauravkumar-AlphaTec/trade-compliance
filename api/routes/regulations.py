"""Regulation endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_db, get_llm
from api.models import (
    RegulationSummary,
    RegulationDetail,
    RegulationListResponse,
    SourceTraceability,
    CountryContext,
    HsCodeResult,
)

router = APIRouter(prefix="/regulations", tags=["regulations"])


def _build_country_context(country_code: str, conn) -> CountryContext:
    """Fetch live country context from kb_country_profiles."""
    cur = conn.cursor()
    try:
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
        row = cur.fetchone()
        if not row:
            return CountryContext(iso2=country_code, country_name=country_code)

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
    finally:
        cur.close()

    return CountryContext(
        iso2=row["iso2"],
        country_name=row["country_name"],
        block_code=row["block_code"],
        block_name=row["block_name"],
        memberships=memberships,
    )


def _build_source(source_row: dict) -> SourceTraceability:
    return SourceTraceability(
        source_name=source_row.get("source_name", ""),
        document_id=source_row.get("document_id", ""),
        url=source_row.get("url"),
        fetched_at=source_row.get("fetched_at"),
    )


@router.get("", response_model=RegulationListResponse)
def list_regulations(
    country: str | None = Query(None, description="Filter by country code"),
    document_type: str | None = Query(None),
    q: str | None = Query(None, description="Full-text keyword search"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    conn=Depends(get_db),
):
    """List and search regulations."""
    conditions = []
    params: list = []

    if country:
        conditions.append("r.country = %s")
        params.append(country)
    if document_type:
        conditions.append("r.document_type = %s")
        params.append(document_type)
    if q:
        conditions.append(
            "to_tsvector('english', r.title || ' ' || COALESCE(r.full_text, '')) "
            "@@ plainto_tsquery('english', %s)"
        )
        params.append(q)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    cur = conn.cursor()
    try:
        # Count
        cur.execute(f"SELECT COUNT(*) AS cnt FROM regulations r {where}", params)
        total = cur.fetchone()["cnt"]

        # Fetch page
        cur.execute(
            f"""
            SELECT r.id, r.title, r.document_type, r.authority,
                   r.country, r.effective_date, r.status
            FROM regulations r
            {where}
            ORDER BY r.effective_date DESC NULLS LAST, r.id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    regulations = [RegulationSummary(**row) for row in rows]
    return RegulationListResponse(regulations=regulations, total=total)


@router.get("/{regulation_id}", response_model=RegulationDetail)
def get_regulation(
    regulation_id: int,
    conn=Depends(get_db),
):
    """Get a single regulation with source traceability and country context."""
    cur = conn.cursor()
    try:
        # Regulation + source join
        cur.execute(
            """
            SELECT r.id, r.title, r.document_type, r.authority,
                   r.country, r.effective_date, r.expiry_date,
                   r.summary, r.status, r.created_at, r.updated_at,
                   s.source_name, s.document_id, s.url, s.fetched_at
            FROM regulations r
            LEFT JOIN sources s ON r.source_id = s.id
            WHERE r.id = %s
            """,
            (regulation_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Regulation not found")

        # HS codes
        cur.execute(
            """
            SELECT hc.code, hc.code_type, hc.description,
                   rhc.confidence
            FROM regulation_hs_codes rhc
            JOIN kb_hs_codes hc ON rhc.hs_code_id = hc.id
            WHERE rhc.regulation_id = %s
            ORDER BY rhc.confidence DESC
            """,
            (regulation_id,),
        )
        hs_rows = cur.fetchall()
    finally:
        cur.close()

    source = SourceTraceability(
        source_name=row.get("source_name", ""),
        document_id=row.get("document_id", ""),
        url=row.get("url"),
        fetched_at=row.get("fetched_at"),
    )

    country_ctx = _build_country_context(row["country"], conn)

    hs_codes = [
        HsCodeResult(
            code=h["code"],
            code_type=h["code_type"],
            description=h["description"],
            confidence=h["confidence"],
        )
        for h in hs_rows
    ]

    return RegulationDetail(
        id=row["id"],
        title=row["title"],
        document_type=row["document_type"],
        authority=row["authority"],
        country=row["country"],
        effective_date=row["effective_date"],
        expiry_date=row["expiry_date"],
        summary=row["summary"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        source=source,
        country_context=country_ctx,
        hs_codes=hs_codes,
    )
