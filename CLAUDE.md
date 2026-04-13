# CLAUDE.md

## Project
Trade compliance system. Two subsystems — Knowledge Base (KB) and Regulation
Pipeline — sharing one database, one Docker stack, and one LLM client.
End goal: given a destination country + product type, return all relevant
regulations, required standards, HS codes, and practical compliance requirements.

## Stack
- Python 3.11
- PostgreSQL 16
- Prefect (self-hosted orchestration)
- FastAPI (internal query API)
- Docker Compose
- Ollama (local inference): gemma4:e4b for all LLM work
- No external API calls in pipeline. Fully self-hosted.

## Non-negotiable rules
1. Never hardcode host URLs. Always read from environment variables.
2. All LLM calls go through pipeline/llm_client.py — never call Ollama directly elsewhere.
3. Every pipeline function and every KB function must have a unit test.
4. All DB writes use parameterised queries. No f-string SQL, ever.
5. Schema changes after v1: add a migration file to db/migrations/. Never edit schema.sql directly.
6. KB and pipeline share the same database. Tables are prefixed: kb_ for KB tables.

## Environment variables required
- OLLAMA_HOST=http://ollama:11434
- DATABASE_URL=postgresql://user:pass@localhost:5432/trade_compliance
- PREFECT_API_URL=http://localhost:4200/api

## Running locally (no Docker)
- PostgreSQL 16 must be installed locally.
- Start postgres: pg_ctl start (or your OS service manager).
- Create the database: createdb trade_compliance
- Init schema: python -m db.init
- Start Ollama: ollama serve (default port 11434)
- Pull model: ollama pull gemma4:e4b
- Start Prefect server: prefect server start  (UI at http://localhost:4200)
- Start API: uvicorn api.main:app --reload    (http://localhost:8000)
- Start pipeline worker: prefect worker start --pool default-agent-pool

## KB design rules
- EU block data lives in kb_block_profiles. Never duplicated per country.
- Germany (DE) is the reference implementation. Validate all patterns there first.
- Every KB field must carry: source_url, confidence (0.0–1.0), last_verified_at.
- Confidence thresholds: ≥0.90 auto-accept | 0.70–0.89 spot-check | <0.70 hold for manual review.
- Free-form text fields always default to confidence=0.65 regardless of model output.

## Source trust tiers (affects confidence calculation)
- Tier 1 (+0.15): WTO.org, ISO.org, BIPM.int, ILAC official, IAF official
- Tier 2 (+0.05): National standards body official website
- Tier 3 (+0.00): Government ministry or regulatory body portal
- Tier 4 (−0.10): Secondary sources, reports, third-party databases

## HS code classification
Classifier in pipeline/hs_classifier.py uses RAG:
1. Embed product description
2. PostgreSQL full-text search against kb_hs_codes filtered by country scope
3. Top-10 candidates + description → gemma4:e4b → {code, confidence, reasoning}
4. confidence < 0.75 → flagged to validation_queue, not rejected
5. classify_hs_code() multiplies raw confidence by 0.85 to account for local model accuracy

## Review CLI
pipeline/review_cli.py handles both validation_queue (pipeline) and
kb_verification_queue (KB). Use 'pipeline' and 'kb' subcommands.
