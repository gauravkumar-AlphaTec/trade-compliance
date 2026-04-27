# Trade Compliance

Given a destination country and product type, returns all relevant regulations,
required standards, HS codes, and practical compliance requirements.

Two subsystems share a single PostgreSQL database, one Docker stack, and one
LLM client:

- **Knowledge Base (KB)** — country and economic-block profiles, memberships,
  standards bodies, accreditation bodies, MRAs, HS-code library, and verified
  product → HS mappings. Tables prefixed `kb_`.
- **Regulation Pipeline** — ingests regulations from EUR-Lex, Federal Register,
  and other government portals; extracts structured fields; classifies product
  categories to HS codes via RAG; routes low-confidence records to a review
  queue.
- **API** — FastAPI query layer over the shared database, plus a single-page
  demo UI at `/`.

## Stack

- Python 3.11
- PostgreSQL 16
- Prefect (self-hosted orchestration)
- FastAPI
- Ollama (local inference) — `gemma4:e2b` for all LLM work
- Docker Compose (optional — local setup works without it)

No external LLM API calls. Fully self-hosted.

## Repository layout

```
api/                FastAPI app, routes, Pydantic models, demo UI
  main.py            app + lifespan + static mount
  deps.py            DB + LLM dependency providers
  routes/            countries, regulations, hs_codes
  static/index.html  single-page demo UI

pipeline/           Regulation ingestion pipeline
  llm_client.py      sole entry point for Ollama inference
  hs_classifier.py   two-tier HS classification (lookup → GIN+LLM)
  extract.py         LLM-driven field extraction
  process.py         normalisation + confidence routing
  validate.py        routes records into auto-accept / spot-check / quarantine
  load.py            parameterised writes into regulations table
  review_cli.py      rich-based TUI for validation_queue + kb_verification_queue
  sources/           eurlex, federal_register, manual loaders

kb/                 Knowledge-base subsystem
  seed_reference.py  hand-curated EU + Germany reference seed
  extract_profile.py LLM extraction for country profiles
  load_profile.py    KB writes + EU_MEMBER_STATES list
  score_confidence.py source-tier adjustments
  diff_profile.py    change detection against existing KB rows
  sources/           bipm, eu_block, gov_portals, ilac_iaf, iso_members,
                     nsb_scraper, wto

hs_library/         Bulk HS code loaders
  load_wco.py        WCO 6-digit chapter/heading/subheading
  load_eu_cn.py      EU Combined Nomenclature (8-digit)
  build_library.py   orchestrator

flows/              Prefect flows
  full_pipeline.py
  ingest_eurlex.py / ingest_us.py
  enrich_block_profile.py / enrich_country_profile.py

db/
  schema.sql         full schema (17 tables)
  init.py            initialises the database from schema.sql
  migrations/        post-v1 schema changes

tests/              pytest suite (one test file per module)
CLAUDE.md           project rules + local setup
ARCHITECTURE.md     design decisions and data flow
```

## Environment variables

```
OLLAMA_HOST=http://localhost:11434
DATABASE_URL=postgresql://user:pass@localhost:5432/trade_compliance
PREFECT_API_URL=http://localhost:4200/api
```

Never hardcode host URLs anywhere in code — always read from the environment.

## Quickstart — run the app

Assumes Postgres is already running and the schema is initialised.

```bash
# Set env (or source .env)
export DATABASE_URL=postgresql://postgres:<password>@localhost:5432/trade_compliance
export OLLAMA_HOST=http://localhost:11434

# Start the API
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

Then open:

- `http://127.0.0.1:8000/`        demo UI
- `http://127.0.0.1:8000/docs`    OpenAPI
- `http://127.0.0.1:8000/health`  liveness

Ollama is only needed for endpoints that classify HS codes (`/hs-codes/search`,
`/hs-codes/compliance-check`). Read-only endpoints work without it.

For first-time setup (Postgres install, schema init, seed, Ollama), see the
full sequence below.

## Running locally (no Docker)

```bash
# 1. PostgreSQL 16 installed locally
pg_ctl start
createdb trade_compliance
python -m db.init                         # applies db/schema.sql

# 2. Ollama
ollama serve                              # default port 11434
ollama pull gemma4:e2b

# 3. Seed reference data (EU block + Germany, no LLM)
python -m kb.seed_reference

# 4. API
uvicorn api.main:app --reload             # http://localhost:8000
#   - /health                             liveness
#   - /                                    demo UI
#   - /docs                                OpenAPI

# 5. Prefect (optional — only needed to run ingest flows)
prefect server start                      # UI at http://localhost:4200
prefect worker start --pool default-agent-pool
```

## Demo UI

Open `http://localhost:8000/` after starting the API. Single panel, two-step
flow for DE:

1. Type a product description → ranked HS code candidates from the loaded
   library (GIN full-text search, no LLM).
2. Click a candidate → regulations + per-directive notified bodies and
   harmonised standards in force (pure SQL, no LLM).

See `COMPLIANCE_FLOW.md` for the end-to-end DE query chain, ingestion
diagrams, and the DB schema (mermaid).

## Tests

```bash
pytest                                    # full suite
pytest tests/test_llm_client.py -q        # single module
```

All pipeline and KB functions have a corresponding unit test. All Ollama calls
in tests are mocked — the suite does not require a running LLM.

## Key rules (see `CLAUDE.md`)

1. Never hardcode host URLs — read from environment.
2. All LLM calls go through `pipeline/llm_client.py`.
3. Every pipeline and KB function has a unit test.
4. All DB writes use parameterised queries. No f-string SQL.
5. Schema changes after v1 → add a file to `db/migrations/`. Never edit
   `schema.sql` directly.
6. KB and pipeline share one database; KB tables prefixed `kb_`.

## Confidence routing

All KB fields carry `source_url`, `confidence` (0.0–1.0), `last_verified_at`.

| Band       | Action              |
|------------|---------------------|
| ≥ 0.90     | auto-accept         |
| 0.70–0.89  | spot-check          |
| < 0.70     | hold (manual review)|

Free-form text always defaults to 0.65 regardless of model output.
`classify_hs_code()` multiplies the raw model confidence by 0.85 to account
for local-model accuracy.

Source-tier adjustments (applied in `kb/score_confidence.py`):

| Tier | Examples                                  | Adjustment |
|------|-------------------------------------------|------------|
| 1    | WTO.org, ISO.org, BIPM.int, ILAC, IAF     | +0.15      |
| 2    | National standards body official site     | +0.05      |
| 3    | Government ministry / regulator portal    |  0.00      |
| 4    | Secondary / third-party databases         | −0.10      |

See `ARCHITECTURE.md` for component-level design decisions and data flow,
and `COMPLIANCE_FLOW.md` for the DE end-to-end query chain with diagrams.
