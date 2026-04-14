-- ══════════════════════════════════════════════
-- KB: NOTIFIED BODIES
-- EU notified bodies (from NANDO / SMCS).
-- A notified body is authorised to perform conformity assessment for
-- one or more directives. Identified by a 4-digit number stamped on the
-- CE marking certificate.
-- ══════════════════════════════════════════════
CREATE TABLE kb_notified_bodies (
    id                 SERIAL PRIMARY KEY,
    nb_number          TEXT NOT NULL UNIQUE,    -- 4-digit identifier e.g. '0197'
    name               TEXT NOT NULL,
    country_id         INTEGER REFERENCES kb_country_profiles(id),
    city               TEXT,
    email              TEXT,
    website            TEXT,
    accreditation_body TEXT,                    -- 'DAkkS' for DE, varies per country
    status             TEXT DEFAULT 'active',   -- 'active','withdrawn','suspended'
    source_url         TEXT NOT NULL,
    confidence         FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at   TIMESTAMPTZ,
    extraction_method  TEXT                     -- 'manual','csv_import','nando_pdf'
);

-- ══════════════════════════════════════════════
-- KB: NOTIFIED BODY ↔ DIRECTIVE DESIGNATIONS
-- Many-to-many. Per-designation fields (authority, approval date,
-- standards) live here because they vary by directive for the same body.
-- ══════════════════════════════════════════════
CREATE TABLE kb_notified_body_directives (
    id                   SERIAL PRIMARY KEY,
    nb_id                INTEGER NOT NULL REFERENCES kb_notified_bodies(id) ON DELETE CASCADE,
    directive_ref        TEXT NOT NULL,            -- '2006/42/EC', '2017/745'
    directive_name       TEXT,                     -- 'Machinery Directive'
    notifying_authority  TEXT,                     -- 'ZLS', 'BfArM', 'BNetzA', ...
    last_approval_date   DATE,
    assessment_standards TEXT[],                   -- ['EN ISO/IEC 17025', 'EN ISO/IEC 17065']
    scope_notes          TEXT,
    source_url           TEXT,
    confidence           FLOAT NOT NULL DEFAULT 0.5,
    last_verified_at     TIMESTAMPTZ,
    UNIQUE(nb_id, directive_ref)
);

CREATE INDEX ON kb_notified_bodies (country_id, status);
CREATE INDEX ON kb_notified_body_directives (directive_ref);

-- ══════════════════════════════════════════════
-- Close the regulations ↔ notified-bodies loop.
-- The title column encodes the directive number in prose; we add a
-- clean join key so HS classification can chain to notified bodies via
-- regulations.directive_ref = kb_notified_body_directives.directive_ref.
-- ══════════════════════════════════════════════
ALTER TABLE regulations ADD COLUMN directive_ref TEXT;
CREATE INDEX ON regulations (directive_ref);

-- Backfill the 5 seeded directives. Pattern-match is mojibake-safe
-- (em-dash separator renders differently across consoles but doesn't
-- affect LIKE matching on the prefix).
UPDATE regulations SET directive_ref = '2006/42/EC' WHERE title LIKE 'Directive 2006/42/EC%';
UPDATE regulations SET directive_ref = '2014/35/EU' WHERE title LIKE 'Directive 2014/35/EU%';
UPDATE regulations SET directive_ref = '2014/30/EU' WHERE title LIKE 'Directive 2014/30/EU%';
UPDATE regulations SET directive_ref = '2017/745'   WHERE title LIKE 'Regulation (EU) 2017/745%';
UPDATE regulations SET directive_ref = '2011/65/EU' WHERE title LIKE 'Directive 2011/65/EU%';
