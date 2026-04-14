-- ══════════════════════════════════════════════
-- KB: HARMONISED STANDARDS (EU NLF)
-- One row per (standard, directive) pair. The same EN standard can be
-- harmonised under multiple directives (e.g. EN 60204-1 under both
-- Machinery and LVD), so the natural key is composite. Source: the
-- per-directive "Summary list" XLSX published by the EU Commission at
-- single-market-economy.ec.europa.eu/.../harmonised-standards/.
-- ══════════════════════════════════════════════
CREATE TABLE kb_harmonised_standards (
    id                 SERIAL PRIMARY KEY,
    standard_code      TEXT NOT NULL,           -- 'EN ISO 12100:2010'
    title              TEXT,
    eso                TEXT,                    -- 'CEN', 'Cenelec', 'ETSI'
    directive_ref      TEXT NOT NULL,           -- '2006/42/EC'
    in_force_from      DATE,                    -- start of presumption of conformity
    withdrawn_on       DATE,                    -- end of presumption of conformity (NULL = still in force)
    oj_publication_ref TEXT,                    -- 'OJ L 366 - 15/10/2021'
    oj_withdrawal_ref  TEXT,
    source_url         TEXT NOT NULL,
    confidence         FLOAT NOT NULL DEFAULT 0.92,
    last_verified_at   TIMESTAMPTZ,
    extraction_method  TEXT,                    -- 'oj_xlsx'
    UNIQUE(standard_code, directive_ref)
);

CREATE INDEX ON kb_harmonised_standards (directive_ref);
CREATE INDEX ON kb_harmonised_standards (standard_code);
-- Partial index on rows currently in force, the common query.
CREATE INDEX ON kb_harmonised_standards (directive_ref) WHERE withdrawn_on IS NULL;
