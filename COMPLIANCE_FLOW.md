# Compliance flow — DE end-to-end

How the system answers "what do I need to sell this product in Germany?"
using only data loaded into the KB — no LLM required at query time.

## 1. What gets loaded

Three authoritative sources feed the DE knowledge base:

| Source | Format | Loader | Target tables |
|---|---|---|---|
| EU Combined Nomenclature | CSV | `hs_library/load_eu_cn.py` | `kb_hs_codes` (EU_CN_8) |
| WCO HS headings | CSV | `hs_library/load_wco.py` | `kb_hs_codes` (WCO_6) |
| EUR-Lex regulations (DE-relevant directives) | hand-curated seed | `kb/seed_reference.py` | `regulations`, `regulation_hs_codes` |
| NANDO notified-body PDFs | 328 PDFs | `kb/load_notified_bodies_from_pdfs.py` | `kb_notified_bodies`, `kb_notified_body_directives` |
| EU harmonised standards | 5 XLSX (one per directive) | `kb/load_harmonised_standards.py` | `kb_harmonised_standards` |

Loaded volumes for DE as of last run:

- **5** regulations (Machinery, LVD, EMC, MDR, RoHS)
- **186** unique notified bodies / **324** designations across **30** directives
- **2,425** harmonised-standard rows (**1,579** currently in force)
- **~9,500** HS codes (WCO 6-digit + EU CN 8-digit)

## 2. Database schema — how the tables connect

```mermaid
erDiagram
    kb_country_profiles ||--o{ kb_memberships : "belongs to orgs"
    kb_country_profiles ||--o{ kb_notified_bodies : "hosts"
    kb_country_profiles }o--|| kb_economic_blocks : "in block"

    kb_hs_codes ||--o{ regulation_hs_codes : "scoped by"
    regulations ||--o{ regulation_hs_codes : "applies to"
    regulations }o--|| kb_country_profiles : "country"
    regulations }o..o| directive : "directive_ref"

    kb_notified_bodies ||--o{ kb_notified_body_directives : "designated for"
    kb_notified_body_directives }o..o| directive : "directive_ref"

    kb_harmonised_standards }o..o| directive : "directive_ref"

    kb_country_profiles {
        int id PK
        string iso2
        string country_name
        int block_id FK
    }
    kb_economic_blocks {
        int id PK
        string code
        string name
    }
    kb_memberships {
        int id PK
        int country_id FK
        string org_code
        bool is_member
    }
    kb_hs_codes {
        int id PK
        string code
        string code_type
        text description
        tsvector search_vector
    }
    regulations {
        int id PK
        string country
        string title
        string directive_ref
        string document_type
        date effective_date
    }
    regulation_hs_codes {
        int regulation_id FK
        int hs_code_id FK
    }
    kb_notified_bodies {
        int id PK
        string nb_number
        string name
        int country_id FK
        string status
    }
    kb_notified_body_directives {
        int id PK
        int nb_id FK
        string directive_ref
        string notifying_authority
        date last_approval_date
    }
    kb_harmonised_standards {
        int id PK
        string standard_code
        text title
        string eso
        string directive_ref
        date in_force_from
        date withdrawn_on
    }
```

Key join keys:

- `regulation_hs_codes.hs_code_id → kb_hs_codes.id` links regulations to HS codes.
  Prefix match: `hc.code = LEFT(product_hs_code, LENGTH(hc.code))` so a chapter-level regulation (`84`) still matches an 8-digit product (`84135030`).
- `regulations.directive_ref` is the string-level join key (`2006/42/EC`, `2014/35/EU`, …) against both `kb_notified_body_directives.directive_ref` and `kb_harmonised_standards.directive_ref`. There is no foreign key to a directives table; the `directive_ref` string *is* the identity.
- `kb_harmonised_standards` has a partial index `WHERE withdrawn_on IS NULL` — the "in force" query is O(log n) on that partial index.

## 3. Query flow — product to full compliance answer

```mermaid
flowchart TD
    U[User: product description + country] --> C1[POST /hs-codes/candidates]
    C1 --> C2{GIN full-text search<br/>kb_hs_codes filtered by<br/>country_scope EU_CN_8}
    C2 --> C3[Ranked HS candidates<br/>returned to UI]
    C3 --> U2[User picks one candidate]
    U2 --> D1[POST /hs-codes/compliance-by-code]

    D1 --> Q1[Query 1: country context]
    Q1 --> T1[(kb_country_profiles<br/>+ kb_economic_blocks<br/>+ kb_memberships)]

    D1 --> Q2[Query 2: matching regulations]
    Q2 --> T2[(regulations<br/>JOIN regulation_hs_codes<br/>JOIN kb_hs_codes<br/>WHERE country = DE<br/>AND hc.code = LEFT prefix)]
    T2 --> DR[directive_refs<br/>extracted from results]

    DR --> Q3[Query 3: notified bodies<br/>for those directives]
    Q3 --> T3[(kb_notified_bodies<br/>JOIN kb_notified_body_directives<br/>WHERE country = DE<br/>AND directive_ref = ANY)]

    DR --> Q4[Query 4: harmonised standards<br/>in force for those directives]
    Q4 --> T4[(kb_harmonised_standards<br/>WHERE directive_ref = ANY<br/>AND withdrawn_on IS NULL)]

    T1 --> R[Response]
    T2 --> R
    T3 --> R
    T4 --> R
    R --> UI[UI renders per-directive breakdown]
```

Everything after the candidate pick is pure SQL. No LLM call in the hot path — the system answers deterministically from loaded data.

## 4. Ingestion pipelines — how each source reaches the DB

### NANDO notified bodies (328 PDFs → 186 bodies / 324 designations)

```mermaid
flowchart LR
    PDF[NANDO PDF<br/>one per designation] --> P1[pdfplumber extract_text]
    P1 --> P2[extract_notification<br/>kb/sources/nando_pdf.py]
    P2 -->|Legislation line regex| D[directive_ref]
    P2 -->|NB number line| N[nb_number + name]
    P2 -->|Address block| A[city + email + website]
    P2 -->|Mixed-case acronym regex| AU[notifying_authority<br/>ZLS / ZLG / DIBt]
    P2 -->|last row of table| AP[last_approval_date]
    D & N & A & AU & AP --> L[load_notified_bodies_from_pdfs.py]
    L -->|ON CONFLICT nb_number| T1[(kb_notified_bodies)]
    L -->|ON CONFLICT nb_id+directive_ref| T2[(kb_notified_body_directives)]
```

Key fix: `directive_ref` is extracted from the PDF body's `Legislation:` line, **not** from the filename — EU serves the same file under multiple names and downloads can double-suffix extensions.

3 PDFs are legitimately skipped — they are Recognised Third-Party Organisations under the Pressure Equipment Directive and have no NB number.

### EU harmonised standards (5 XLSX → 2,425 rows)

```mermaid
flowchart LR
    X[EU XLSX<br/>one per directive] --> R1{detect schema}
    R1 -->|Schema A: combined<br/>ref+title column| A1[split newline<br/>in Reference and title]
    R1 -->|Schema B: separate<br/>code + title columns| B1[read columns directly]
    A1 --> P[parse_xlsx<br/>kb/sources/harmonised_standards_xlsx.py]
    B1 --> P
    P -->|filename regex| DR[directive_ref<br/>2006_42_EC → 2006/42/EC]
    P --> F[in_force_from<br/>withdrawn_on<br/>eso: CEN/Cenelec/ETSI]
    DR & F --> L[load_harmonised_standards.py]
    L -->|ON CONFLICT<br/>standard_code + directive_ref| T[(kb_harmonised_standards)]
```

The RoHS file (`2011_65_EU.xlsx`) is a legacy OLE `.xls` served under an `.xlsx` name — `_read_excel_any()` falls back from openpyxl to xlrd.

## 5. API surface

| Endpoint | Input | What it exercises |
|---|---|---|
| `GET /countries/{iso2}` | `DE` | `kb_country_profiles` + `kb_economic_blocks` + `kb_memberships` |
| `GET /regulations?country=DE` | `DE` | `regulations` list |
| `POST /hs-codes/search` | `{product_description, country_scope}` | two-tier classifier (lookup → GIN → LLM) |
| `POST /hs-codes/candidates` | `{country_code, product_description, limit}` | **GIN FTS only, no LLM** — powers the UI candidate list |
| `POST /hs-codes/compliance-by-code` | `{country_code, hs_code}` | skips classification, runs the 4 queries in section 3 |
| `POST /hs-codes/compliance-check` | `{country_code, product_description}` | full pipeline: classify + all 4 queries |

`compliance-by-code` is the endpoint the demo UI actually hits after the user picks a candidate. It lets us exercise the DB-to-DB joins without waiting on the local LLM.

## 6. Demo UI

`api/static/index.html` — single file, vanilla JS, no build step. Served at `/`.

Two-step flow:
1. Type a product description → `POST /candidates` → ranked HS codes appear as clickable cards.
2. Click a card → `POST /compliance-by-code` → renders regulations, per-directive breakdown, notified bodies, harmonised standards.

Purpose: surface the 6,000+ HS code library and the full join chain without hardcoded products.

## 7. What each directive governs (DE coverage)

| Directive | Subject | NBs in DE | In-force standards |
|---|---|---|---|
| 2006/42/EC | Machinery | many | ~816 |
| 2014/35/EU | Low Voltage (LVD) | many | ~573 |
| 2014/30/EU | Electromagnetic Compatibility (EMC) | many | ~138 |
| 2017/745 | Medical Devices (MDR) | ZLG-notified | ~51 |
| 2011/65/EU | RoHS | — | ~1 |

## 8. Known limitations

- Standards are filtered by directive, not by HS code. An "electric drill" under the Machinery Directive surfaces all 816 Machinery standards — legally complete, but broad. A future narrowing step can keyword-match standard titles against the product description.
- Only DE is fully loaded. EU block membership is cross-country, but `regulations`, `kb_notified_bodies`, and the DE-specific directive coverage are DE-only.
- `directive_ref` is a string, not a foreign key. A typo during ingest (e.g. `2006/42/CE` vs `2006/42/EC`) would silently break the join. Guarded by regex normalisation in each loader.
- The harmonised-standards XLSXs ship in two schemas; the auto-detect is conservative but could miss a future third schema without a test fixture.
