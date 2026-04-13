-- Add kb_product_hs_mappings table and GIN index on kb_hs_codes.description
-- for full-text search used by HS library builder.

CREATE TABLE IF NOT EXISTS kb_product_hs_mappings (
    id              SERIAL PRIMARY KEY,
    product_category TEXT NOT NULL,
    hs_code_id      INTEGER REFERENCES kb_hs_codes(id),
    hs_code         TEXT NOT NULL,
    code_type       TEXT,
    confidence      FLOAT NOT NULL DEFAULT 0.0,
    reasoning       TEXT,
    national_variant TEXT,
    country_scope   TEXT,
    source          TEXT NOT NULL DEFAULT 'opus_initial',
    verified        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_hs_mappings_category
    ON kb_product_hs_mappings (product_category);
CREATE INDEX IF NOT EXISTS idx_product_hs_mappings_hs_code
    ON kb_product_hs_mappings (hs_code);

-- GIN index for full-text search on kb_hs_codes.description
CREATE INDEX IF NOT EXISTS idx_hs_codes_description_gin
    ON kb_hs_codes USING GIN (to_tsvector('english', description));
