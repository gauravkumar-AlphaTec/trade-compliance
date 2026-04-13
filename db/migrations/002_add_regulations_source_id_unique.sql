-- Add UNIQUE constraint on regulations.source_id to support upsert.
-- Each source document produces exactly one regulation record.

ALTER TABLE regulations
    ADD CONSTRAINT regulations_source_id_unique UNIQUE (source_id);
