"""Pydantic models for API request and response types."""

from datetime import date, datetime

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Shared / nested models
# ------------------------------------------------------------------

class SourceTraceability(BaseModel):
    source_name: str
    document_id: str
    url: str | None = None
    fetched_at: datetime | None = None


class CountryContext(BaseModel):
    iso2: str
    country_name: str
    block_code: str | None = None
    block_name: str | None = None
    memberships: list[str] = Field(default_factory=list)


class HsCodeResult(BaseModel):
    code: str
    code_type: str | None = None
    description: str | None = None
    confidence: float | None = None
    reasoning: str | None = None
    national_variant: str | None = None


# ------------------------------------------------------------------
# Regulation responses
# ------------------------------------------------------------------

class RegulationSummary(BaseModel):
    id: int
    title: str
    document_type: str | None = None
    authority: str | None = None
    country: str
    effective_date: date | None = None
    status: str | None = None


class RegulationDetail(BaseModel):
    id: int
    title: str
    document_type: str | None = None
    authority: str | None = None
    country: str
    effective_date: date | None = None
    expiry_date: date | None = None
    summary: str | None = None
    status: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    source: SourceTraceability
    country_context: CountryContext
    hs_codes: list[HsCodeResult] = Field(default_factory=list)


class RegulationListResponse(BaseModel):
    regulations: list[RegulationSummary]
    total: int


# ------------------------------------------------------------------
# Country responses
# ------------------------------------------------------------------

class MembershipInfo(BaseModel):
    org_code: str
    is_member: bool
    membership_type: str | None = None


class CountryProfileResponse(BaseModel):
    iso2: str
    iso3: str
    country_name: str
    region: str | None = None
    block_code: str | None = None
    block_name: str | None = None
    memberships: list[MembershipInfo] = Field(default_factory=list)
    standards_body: dict | None = None
    accreditation_body: dict | None = None


# ------------------------------------------------------------------
# HS code search
# ------------------------------------------------------------------

class HsCodeSearchRequest(BaseModel):
    product_description: str
    country_scope: str = "WCO"


class HsCodeSearchResponse(BaseModel):
    classification: HsCodeResult
    candidates: list[HsCodeResult] = Field(default_factory=list)


# ------------------------------------------------------------------
# Compliance check
# ------------------------------------------------------------------

class ComplianceCheckRequest(BaseModel):
    country_code: str
    product_description: str


class HarmonisedStandardSummary(BaseModel):
    standard_code: str
    title: str | None = None
    eso: str | None = None
    directive_ref: str
    in_force_from: date | None = None


class NotifiedBodySummary(BaseModel):
    nb_number: str
    name: str
    city: str | None = None
    email: str | None = None
    website: str | None = None
    directive_ref: str
    notifying_authority: str | None = None
    last_approval_date: date | None = None


class ComplianceCheckResponse(BaseModel):
    country_context: CountryContext
    hs_classification: HsCodeResult | None = None
    regulations: list[RegulationSummary] = Field(default_factory=list)
    standards: list[str] = Field(default_factory=list)
    notified_bodies: list[NotifiedBodySummary] = Field(default_factory=list)
    harmonised_standards: list[HarmonisedStandardSummary] = Field(default_factory=list)
