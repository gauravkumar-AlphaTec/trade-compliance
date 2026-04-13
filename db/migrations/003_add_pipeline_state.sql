-- Pipeline state table for tracking last-run timestamps per stage.

CREATE TABLE IF NOT EXISTS pipeline_state (
    id        SERIAL PRIMARY KEY,
    stage     TEXT NOT NULL UNIQUE,
    last_run  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed with initial state for the full pipeline
INSERT INTO pipeline_state (stage, last_run)
VALUES ('full_pipeline', '2020-01-01T00:00:00Z')
ON CONFLICT (stage) DO NOTHING;
