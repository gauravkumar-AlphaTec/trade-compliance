"""Country profile endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_db
from api.models import CountryProfileResponse, MembershipInfo

router = APIRouter(prefix="/countries", tags=["countries"])


@router.get("/{country_code}", response_model=CountryProfileResponse)
def get_country_profile(
    country_code: str,
    conn=Depends(get_db),
):
    """Get a country profile with block and membership info."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT cp.iso2, cp.iso3, cp.country_name, cp.region,
                   cp.national_standards_body, cp.accreditation_body,
                   eb.code AS block_code, eb.name AS block_name
            FROM kb_country_profiles cp
            LEFT JOIN kb_economic_blocks eb ON cp.block_id = eb.id
            WHERE cp.iso2 = %s
            """,
            (country_code,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Country not found")

        cur.execute(
            """
            SELECT org_code, is_member, membership_type
            FROM kb_memberships
            WHERE country_id = (
                SELECT id FROM kb_country_profiles WHERE iso2 = %s
            )
            """,
            (country_code,),
        )
        membership_rows = cur.fetchall()
    finally:
        cur.close()

    memberships = [
        MembershipInfo(
            org_code=m["org_code"],
            is_member=m["is_member"],
            membership_type=m.get("membership_type"),
        )
        for m in membership_rows
    ]

    return CountryProfileResponse(
        iso2=row["iso2"],
        iso3=row["iso3"],
        country_name=row["country_name"],
        region=row.get("region"),
        block_code=row.get("block_code"),
        block_name=row.get("block_name"),
        memberships=memberships,
        standards_body=row.get("national_standards_body"),
        accreditation_body=row.get("accreditation_body"),
    )
