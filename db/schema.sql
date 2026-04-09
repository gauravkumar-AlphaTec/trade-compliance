-- ══════════════════════════════════════════════
-- KB: ECONOMIC BLOCKS
-- EU, ASEAN, GCC, etc. Countries inherit from blocks.
-- ══════════════════════════════════════════════
CREATE TABLE kb_economic_blocks (
    id              SERIAL PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,   -- 'EU', 'ASEAN', 'GCC'
    name            TEXT NOT NULL,
    block_type      TEXT,                   -- 'customs_union', 'free_trade_area'
    official_url    TEXT
);

-- Shared profile data for a block (stored as JSONB because structure varies).
-- EU block stores: harmonized directives, shared MRAs, CE framework.
CREATE TABLE kb_block_profiles (
    id                   SERIAL PRIMARY KEY,
    block_id             INTEGER REFERENCES kb_economic_blocks(id) UNIQUE,
    directives           JSONB DEFAULT '[]',  -- [{number, title, url, scope}]
    harmonized_standards JSONB DEFAULT '[]',  -- [{code, title, directive, ojl_ref}]
    shared_mras          JSONB DEFAULT '[]',
    conformity_framework JSONB DEFAULT '{}',
    last_updated         TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════
-- KB: COUNTRY PROFILES
-- ══════════════════════════════════════════════
CREATE TABLE kb_country_profiles (
    id              SERIAL PRIMARY KEY,
    iso2            TEXT NOT NULL UNIQUE,
    iso3            TEXT NOT NULL UNIQUE,
    country_name    TEXT NOT NULL,
    region          TEXT,
    block_id        INTEGER REFERENCES kb_economic_blocks(id),
    tags            TEXT[],

    -- Quality infrastructure bodies (JSONB with embedded provenance)
    -- Structure: {"value": {name, acronym, url, scope}, "source_url": "...",
    --             "confidence": 0.0, "last_verified_at": "YYYY-MM-DD"}
    national_standards_body         JSONB,
    accreditation_body              JSONB,
    metrology_institute             JSONB,
    legal_metrology_body            JSONB,
    market_surveillance_authorities JSONB DEFAULT '[]',
    notified_bodies_url             TEXT,

    -- Language & documentation
    official_languages              TEXT[],
    accepted_doc_languages          TEXT[],
    translation_requirements        TEXT,   -- 'certified', 'notarized', 'none'
    translation_notes               TEXT,

    -- Conformity assessment
    ca_system_structure             TEXT,   -- 'centralized', 'decentralized'
    accreditation_mandatory         BOOLEAN,
    accepted_certificates           JSONB DEFAULT '{}',
    tech_regulation_refs            JSONB DEFAULT '[]',

    -- Rich text / insights (always queued for manual review regardless of confidence)
    local_challenges                TEXT,
    recent_reforms                  TEXT,
    useful_portals                  JSONB DEFAULT '[]',  -- [{name, url, description}]
    regulatory_deadlines            JSONB DEFAULT '[]',  -- [{description, date}]
    general_notes                   TEXT,

    -- Profile metadata
    profile_version                 INTEGER DEFAULT 1,
    profile_status                  TEXT DEFAULT 'draft',  -- 'draft', 'published', 'archived'
    created_at                      TIMESTAMPTZ DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════
-- KB: INTERNATIONAL MEMBERSHIPS
-- One row per country per organisation. Fully queryable.
-- "Give me all ILAC signatory countries" → fast index scan.
-- ══════════════════════════════════════════════
CREATE TABLE kb_memberships (
    id               SERIAL PRIMARY KEY,
    country_id       INTEGER REFERENCES kb_country_profiles(id),
    org_code         TEXT NOT NULL,   -- 'WTO', 'ISO', 'IEC', 'OIML', 'BIPM', 'ILAC', 'IAF'
    is_member        BOOLEAN NOT NULL,
    membership_type  TEXT,            -- 'full', 'correspondent', 'subscriber', 'observer'
    accession_date   DATE,
    scope_details    TEXT,
    source_url       TEXT NOT NULL,
    confidence       FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at TIMESTAMPTZ,
    extraction_method TEXT,           -- 'direct_parse', 'llm_extract', 'manual'
    UNIQUE(country_id, org_code)
);

-- ══════════════════════════════════════════════
-- KB: MUTUAL RECOGNITION AGREEMENTS
-- ══════════════════════════════════════════════
CREATE TABLE kb_mras (
    id               SERIAL PRIMARY KEY,
    country_id       INTEGER REFERENCES kb_country_profiles(id),
    mra_type         TEXT NOT NULL,   -- 'bilateral', 'multilateral', 'rta_based'
    partner          TEXT,
    name             TEXT,
    scope            TEXT,
    membership_role  TEXT,
    sectors_covered  TEXT[],
    signed_date      DATE,
    effective_date   DATE,
    source_url       TEXT,
    confidence       FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at TIMESTAMPTZ
);

-- ══════════════════════════════════════════════
-- KB: LEGAL & REGULATORY FRAMEWORK
-- ══════════════════════════════════════════════
CREATE TABLE kb_laws (
    id                     SERIAL PRIMARY KEY,
    country_id             INTEGER REFERENCES kb_country_profiles(id),
    title                  TEXT NOT NULL,
    law_type               TEXT,   -- 'national_law', 'regulation', 'decree'
    scope                  TEXT,
    url                    TEXT,
    standards_mandatory    BOOLEAN,
    local_adaptation_notes TEXT,
    source_url             TEXT,
    confidence             FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at       TIMESTAMPTZ
);

CREATE TABLE kb_regulatory_authorities (
    id             SERIAL PRIMARY KEY,
    country_id     INTEGER REFERENCES kb_country_profiles(id),
    name           TEXT NOT NULL,
    acronym        TEXT,
    scope          TEXT,
    url            TEXT,
    authority_type TEXT,  -- 'customs', 'market_surveillance', 'standards', 'sectoral'
    source_url     TEXT,
    confidence     FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at TIMESTAMPTZ
);

-- ══════════════════════════════════════════════
-- KB: STANDARDS ACCEPTANCE
-- One row per standard per country.
-- EU countries: most rows inherited from block, only deviations stored here.
-- ══════════════════════════════════════════════
CREATE TABLE kb_standards_acceptance (
    id                  SERIAL PRIMARY KEY,
    country_id          INTEGER REFERENCES kb_country_profiles(id),
    standard_code       TEXT NOT NULL,  -- 'ISO 12100', 'EN 55032'
    standard_name       TEXT,
    standard_type       TEXT,           -- 'design', 'testing', 'labeling', 'process'
    accepted            BOOLEAN,
    national_equivalent TEXT,
    harmonization_level TEXT,           -- 'full', 'partial', 'none', 'pending'
    comments            TEXT,
    source_url          TEXT,
    confidence          FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at    TIMESTAMPTZ,
    UNIQUE(country_id, standard_code)
);

-- ══════════════════════════════════════════════
-- KB: TESTING PROTOCOL ACCEPTANCE
-- ══════════════════════════════════════════════
CREATE TABLE kb_testing_protocols (
    id                     SERIAL PRIMARY KEY,
    country_id             INTEGER REFERENCES kb_country_profiles(id),
    protocol_name          TEXT NOT NULL,  -- 'CB Scheme', 'IECEx CoC', 'IECEE'
    accepted               BOOLEAN,
    accepted_conditionally BOOLEAN DEFAULT FALSE,
    conditions             JSONB DEFAULT '[]',  -- [{type, description}]
    notes                  TEXT,
    source_url             TEXT,
    confidence             FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at       TIMESTAMPTZ,
    UNIQUE(country_id, protocol_name)
);

-- ══════════════════════════════════════════════
-- KB: NATIONAL DEVIATIONS
-- ══════════════════════════════════════════════
CREATE TABLE kb_national_deviations (
    id                     SERIAL PRIMARY KEY,
    country_id             INTEGER REFERENCES kb_country_profiles(id),
    reference_standard     TEXT NOT NULL,
    deviation_type         TEXT,  -- 'scope', 'method', 'thresholds', 'additional_requirements'
    description            TEXT NOT NULL,
    documentation_required JSONB DEFAULT '[]',
    source_url             TEXT,
    confidence             FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at       TIMESTAMPTZ
);

-- ══════════════════════════════════════════════
-- KB: HS CODE LIBRARY
-- WCO 6-digit base + EU Combined Nomenclature 8-digit + US HTS 10-digit.
-- Also used by the regulation pipeline classifier.
-- ══════════════════════════════════════════════
CREATE TABLE kb_hs_codes (
    id            SERIAL PRIMARY KEY,
    code          TEXT NOT NULL,
    code_type     TEXT NOT NULL,   -- 'WCO_6', 'EU_CN_8', 'US_HTS_10'
    description   TEXT NOT NULL,
    parent_code   TEXT,
    country_scope TEXT,            -- 'EU', 'US', 'WCO'
    valid_from    DATE,
    valid_to      DATE,
    UNIQUE(code, code_type)
);

-- ══════════════════════════════════════════════
-- KB: VERIFICATION QUEUE
-- Items routed here by the confidence scoring system.
-- Reviewed via review_cli.py kb subcommand.
-- ══════════════════════════════════════════════
CREATE TABLE kb_verification_queue (
    id                  SERIAL PRIMARY KEY,
    country_id          INTEGER REFERENCES kb_country_profiles(id),
    table_name          TEXT NOT NULL,
    record_id           INTEGER,
    field_name          TEXT,
    current_value       TEXT,
    proposed_value      TEXT,
    issue_type          TEXT NOT NULL,  -- 'low_confidence', 'source_conflict', 'stale', 'missing', 'spot_check'
    confidence          FLOAT,
    source_url          TEXT,
    conflict_source_url TEXT,
    status              TEXT DEFAULT 'pending',  -- 'pending', 'resolved', 'dismissed'
    reviewer_note       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

-- ══════════════════════════════════════════════
-- PIPELINE: SOURCE PROVENANCE
-- Every regulation record traces back here.
-- ══════════════════════════════════════════════
CREATE TABLE sources (
    id           SERIAL PRIMARY KEY,
    source_name  TEXT NOT NULL,  -- 'EUR-Lex', 'Federal Register', 'eCFR'
    document_id  TEXT NOT NULL,  -- CELEX number or FR document number
    url          TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL,
    content_hash TEXT,
    UNIQUE(source_name, document_id)
);

-- ══════════════════════════════════════════════
-- PIPELINE: REGULATIONS
-- Core content table.
-- ══════════════════════════════════════════════
CREATE TABLE regulations (
    id             SERIAL PRIMARY KEY,
    source_id      INTEGER REFERENCES sources(id),
    title          TEXT NOT NULL,
    document_type  TEXT,   -- 'regulation', 'directive', 'rule', 'order', 'standard'
    authority      TEXT,
    country        TEXT NOT NULL,  -- ISO2 or 'EU'
    effective_date DATE,
    expiry_date    DATE,
    full_text      TEXT,
    summary        TEXT,           -- LLM-generated
    status         TEXT DEFAULT 'active',  -- 'active', 'superseded', 'draft'
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════
-- PIPELINE: REGULATION ↔ HS CODE MAPPING
-- ══════════════════════════════════════════════
CREATE TABLE regulation_hs_codes (
    regulation_id  INTEGER REFERENCES regulations(id),
    hs_code_id     INTEGER REFERENCES kb_hs_codes(id),
    confidence     FLOAT,
    mapping_method TEXT,   -- 'llm_rag', 'manual', 'rule_based'
    mapped_at      TIMESTAMPTZ DEFAULT NOW(),
    reviewed       BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (regulation_id, hs_code_id)
);

-- ══════════════════════════════════════════════
-- PIPELINE: VALIDATION QUEUE
-- Records that failed automated checks or need human review.
-- Reviewed via review_cli.py pipeline subcommand.
-- ══════════════════════════════════════════════
CREATE TABLE validation_queue (
    id           SERIAL PRIMARY KEY,
    record_type  TEXT NOT NULL,  -- 'regulation', 'hs_mapping'
    record_id    INTEGER,
    issue_type   TEXT NOT NULL,  -- 'missing_field', 'low_confidence', 'corrupt_data'
    issue_detail TEXT,
    status       TEXT DEFAULT 'pending',
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════
-- PIPELINE: RUN LOG
-- One row per pipeline / flow execution for audit trail.
-- ══════════════════════════════════════════════
CREATE TABLE pipeline_runs (
    id              SERIAL PRIMARY KEY,
    flow_name       TEXT NOT NULL,
    run_id          TEXT NOT NULL UNIQUE,  -- Prefect flow-run ID
    status          TEXT DEFAULT 'running',  -- 'running', 'completed', 'failed'
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

-- ══════════════════════════════════════════════
-- INDEXES
-- ══════════════════════════════════════════════
-- KB
CREATE INDEX ON kb_country_profiles (iso2);
CREATE INDEX ON kb_country_profiles (block_id);
CREATE INDEX ON kb_memberships (country_id, org_code);
CREATE INDEX ON kb_memberships (org_code, is_member);
CREATE INDEX ON kb_standards_acceptance (country_id, standard_code);
CREATE INDEX ON kb_standards_acceptance (standard_code, accepted);
CREATE INDEX ON kb_verification_queue (status, country_id);
-- Pipeline
CREATE INDEX ON regulations (country, status);
CREATE INDEX ON regulations (effective_date);
CREATE INDEX ON regulations USING GIN (to_tsvector('english', title || ' ' || COALESCE(full_text, '')));
CREATE INDEX ON validation_queue (status);