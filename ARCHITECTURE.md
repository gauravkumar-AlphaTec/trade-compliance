# Architecture

This document covers the high-level design of the Trade Compliance system:
how the two subsystems (Knowledge Base and Regulation Pipeline) interact,
how data flows from source to query, and the non-obvious design decisions
behind them.

## 1. Goal

Given a destination country (ISO2) and a product description, return:

- The most likely HS code (with confidence and reasoning).
- The country's regulatory infrastructure — economic block, memberships,
  standards body, accreditation body, metrology institute.
- The regulations in force for that country that apply to that HS code.
- The standards accepted by that country (and whether they are mandatory).

This is delivered as a single API call: `POST /hs-codes/compliance-check`.

## 2. Two subsystems, one database

The system is deliberately split into a slow-changing **Knowledge Base** and
a fast-changing **Regulation Pipeline**, but both write to the same
PostgreSQL database so that queries can join across them without an
integration layer.

```
                ┌──────────────────────────────┐
                │        PostgreSQL 16         │
                │                              │
   KB tables ───┤  kb_country_profiles         ├─── regulations
   (prefix kb_) │  kb_economic_blocks          │    regulation_hs_codes
                │  kb_memberships              │    validation_queue
                │  kb_laws                     │
                │  kb_authorities              │
                │  kb_mras                     │
                │  kb_standards_acceptance     │
                │  kb_hs_codes                 │
                │  kb_product_hs_mappings      │
                │  kb_verification_queue       │
                └──────────────────────────────┘
                            ▲          ▲
                            │          │
              ┌─────────────┘          └──────────────┐
              │                                       │
   ┌──────────────────────┐             ┌──────────────────────┐
   │     KB subsystem     │             │   Regulation Pipeline │
   │  (slow, curated)     │             │  (periodic ingests)   │
   └──────────────────────┘             └──────────────────────┘
              ▲                                       ▲
              │                                       │
        hand-curated seed                   EUR-Lex / Federal Register
        + official sources                  + manual uploads
        (BIPM, ISO, WTO, ILAC/IAF,
         national bodies)
```

Germany (DE) is the reference implementation. All new patterns are validated
against DE first before being generalised. EU block data lives once in
`kb_block_profiles`; it is never duplicated per country.

## 3. The LLM boundary

All inference goes through `pipeline/llm_client.py`. No other module calls
Ollama directly. This exists to enforce:

- A single, swappable inference backend (switch models in one place).
- Centralised JSON-tolerance (`_extract_json`) — smaller local models often
  wrap output in ```json fences or add preambles; the client strips these
  before parsing.
- Centralised confidence scaling. `classify_hs_code()` multiplies the raw
  model confidence by **0.85** to account for the lower accuracy of local
  models compared to frontier ones; callers see the scaled value.
- Shared timeout (240 s) and retry-on-bad-JSON loop (3 attempts).

Model: `gemma4:e2b` — chosen for hardware-limited deployments. Anything
larger is out of scope for the current host.

## 4. Two-tier HS classification

The classifier in `pipeline/hs_classifier.py` trades accuracy off against
latency:

```
  product description + country
              │
              ▼
   ┌──────────────────────────┐
   │ 1. lookup_mapping()      │   ← kb_product_hs_mappings, verified=TRUE
   │    Verified mapping?     │     ~1 ms
   └──────────┬───────────────┘
              │ miss
              ▼
   ┌──────────────────────────┐
   │ 2. get_candidates_by_    │   ← kb_hs_codes, GIN full-text search
   │    keyword()             │     filtered by country_scope
   │    top-N candidates      │     ~10 ms
   └──────────┬───────────────┘
              │
              ▼
   ┌──────────────────────────┐
   │ 3. llm.classify_hs_code()│   ← gemma4:e2b, top-N + desc
   │    pick best, explain    │     ~5-60 s
   └──────────┬───────────────┘
              │
              ▼
   ┌──────────────────────────┐
   │ 4. store_new_mapping()   │   ← kb_product_hs_mappings, verified=FALSE
   │    queue_for_review()    │     validation_queue
   └──────────────────────────┘
```

The tiered design means a hot product (e.g. "centrifugal pumps") returns
instantly after its first classification. Only truly new descriptions pay
the LLM cost. New mappings start as `verified=FALSE` and are promoted only
via `review_cli.py`.

**country_scope** — EU members → `EU_CN_8`, US → `US_HTS_10`, else `WCO_6`.
The WCO 6-digit library is always included as a fallback. Getting this
right matters: a seed written with `country_scope='EU'` will not match
against a DE query that expands to `EU_CN_8` (this caused a bug during
early seeding).

**Prefix matching for regulations**: compliance-check joins
`regulation_hs_codes.hs_code_id` → `kb_hs_codes.code` and matches via
`hc.code = LEFT(product_hs_code, LENGTH(hc.code))`. A regulation tagged at
the chapter level (e.g. `84`) still matches an 8-digit product code
(`84135030`). This is intentional: regulations are usually scoped at
chapter or heading granularity, not subheading.

## 5. Confidence routing

Every KB field and pipeline record carries a confidence score. Three bands
drive routing:

| Band       | What happens                                                  |
|------------|---------------------------------------------------------------|
| ≥ 0.90     | auto-accept — write straight to the live table                |
| 0.70–0.89  | spot-check — write, but flag for sampling-based review        |
| < 0.70     | hold — write to the verification/validation queue, not live   |

Scoring rules:
- Free-form text fields default to **0.65** regardless of what the model
  reports — local-model prose is not trustworthy enough to self-score.
- Source-tier adjustments (Tier 1 + 0.15 … Tier 4 − 0.10) are applied in
  `kb/score_confidence.py`.
- HS classification scaling: raw × 0.85 (inside `llm_client`).

## 6. Data flow: ingestion

```
  source (EUR-Lex / Federal Register / gov portal)
                  │
                  ▼
        pipeline/sources/*.py       fetch raw documents
                  │
                  ▼
        pipeline/extract.py         LLM → structured fields
                  │
                  ▼
        pipeline/process.py         normalise country codes, dates,
                                    product categories
                  │
                  ▼
        pipeline/validate.py        route by confidence:
                  │                  auto_accept → load
                  │                  spot_check  → load + flag
                  │                  hold        → validation_queue
                  ▼
        pipeline/load.py            parameterised INSERT into
                                    regulations + regulation_hs_codes
                  │
                  ▼
        hs_classifier.classify_regulation()
                                    classifies each product_category,
                                    links to kb_hs_codes
```

Prefect flows in `flows/` compose these stages. They can run against a
real Prefect server or ephemerally (`PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true`)
for ad-hoc runs.

## 7. Data flow: query

```
  client
    │  POST /hs-codes/compliance-check
    │       { country_code, product_description }
    ▼
  api/routes/hs_codes.py
    │
    ├─► kb_country_profiles      ── country_context
    │   + kb_economic_blocks
    │   + kb_memberships
    │
    ├─► hs_classifier            ── hs_classification
    │    (two-tier)                 {code, confidence, reasoning}
    │
    ├─► regulations JOIN            ── regulations[]
    │   regulation_hs_codes
    │   JOIN kb_hs_codes
    │   WHERE country = ?
    │     AND hc.code = LEFT(hs_code, LENGTH(hc.code))
    │
    └─► kb_standards_acceptance  ── standards[]
        JOIN kb_country_profiles
```

All four reads happen in one request under a single DB connection opened by
`api/deps.py::get_db` (RealDictCursor). The LLM client is held as a
module-level singleton initialised in the FastAPI lifespan — one client for
the life of the process, not per request.

## 8. Review CLI

`pipeline/review_cli.py` is the human-in-the-loop interface. It handles two
queues:

- `validation_queue` — pipeline records held or spot-checked.
- `kb_verification_queue` — KB extractions held below 0.70.

Subcommands: `pipeline` and `kb`. The CLI uses `rich` for tables and inline
diffs. Approving an item promotes it to the live table with `verified=TRUE`
(for HS mappings) or writes the accepted values into the target KB row.

## 9. Design decisions worth calling out

- **One database, not two.** The KB and the pipeline could have lived in
  separate stores with a sync job between them. They don't, because every
  useful query joins across both (country → regulation → HS code →
  standards), and a sync job would have been a permanent source of drift.
- **Hand-curated seed for DE + EU.** Scrapers for tier-1 sources are
  partially broken (only BIPM is reliable). Rather than block progress on
  scraper fixes, `kb/seed_reference.py` seeds DE + EU reference data by
  hand, enough to exercise the full query path end-to-end. Scraper work is
  tracked separately.
- **GIN full-text search, not embeddings.** PostgreSQL's built-in
  `to_tsvector` + `plainto_tsquery` + `ts_rank` is good enough for HS code
  candidate retrieval (descriptions are short and vocabulary is
  constrained). Avoiding pgvector keeps the stack lean.
- **Demo UI in vanilla JS.** `api/static/index.html` is a single file with
  no build step. It exists to make the backend demoable without adding a
  frontend toolchain to the project.
- **No external LLM.** Ollama-only. Trade compliance data is sometimes
  sensitive (supplier routing, product specs); running locally is the
  default safe choice.

## 10. Known limitations

- Only DE + EU are seeded. Other countries return 404 from the query API
  until their profiles are populated.
- Most tier-1 scrapers in `kb/sources/` are incomplete; only BIPM is
  production-grade.
- `gemma4:e2b` is small; HS classification accuracy on the LLM tier is
  around 0.7–0.85 post-scaling. This is why new mappings are queued for
  human review rather than auto-trusted.
- No authn/authz on the API — it is intended to run behind a private
  network boundary.
