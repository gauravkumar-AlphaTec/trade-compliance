# Database Schema & Data Retrieval Logic

How the trade compliance system stores data and retrieves it to answer: **"What EU regulations, harmonised standards, and notified bodies apply to this product in Germany?"**

---

## Core Concept: The Query Chain

A user provides a **product description** (e.g. "electric toy train"). The system must return all applicable EU directives, the harmonised standards that prove conformity, and the notified bodies authorised to certify the product.

The data flows through this chain:

```
Product Description
       │
       ▼
┌─────────────┐    full-text search
│ kb_hs_codes  │◄── against description
└──────┬──────┘
       │  8-digit code (e.g. 95030030)
       │
       ▼  PREFIX MATCH: LEFT('95030030', 4) = '9503'
┌──────────────────┐
│regulation_hs_codes│── links 4-digit heading ──► ┌────────────┐
└──────────────────┘                              │ regulations │
                                                  └──────┬─────┘
                                                         │ directive_ref
                                          ┌──────────────┼──────────────┐
                                          ▼              ▼              ▼
                                   ┌────────────┐ ┌───────────┐ ┌──────────────┐
                                   │ harmonised  │ │ notified  │ │ notified_body│
                                   │ _standards  │ │ _bodies   │ │ _directives  │
                                   └────────────┘ └───────────┘ └──────────────┘
```

The critical join is the **prefix match**: a regulation linked to 4-digit heading `9503` matches any 8-digit product code starting with `9503` (e.g. `95030030`, `95030070`). This is how a single directive-to-heading mapping covers thousands of specific product codes.

---

## Tables With Real Data

### 1. `kb_hs_codes` — HS Code Library (19,640 rows)

The Harmonised System code hierarchy. Every product traded internationally has an HS code.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `code` | text | The HS/CN code (e.g. `9503`, `95030030`) |
| `code_type` | text | Hierarchy level — see below |
| `description` | text | Official description of what this code covers |
| `parent_code` | text | Parent code (e.g. `95030030` → parent `9503`) |
| `country_scope` | text | `WCO` (international) or `EU` (EU-specific 8-digit codes) |

**Code types in the system:**

| code_type | Digits | Count | Example | Meaning |
|-----------|--------|-------|---------|---------|
| `WCO_2` | 2 | 97 | `95` | Chapter (Toys, games, sports equipment) |
| `WCO_4` | 4 | 1,229 | `9503` | Heading (Toys) |
| `WCO_6` | 6 | 5,613 | `950300` | Subheading (international standard) |
| `EU_CN_2` | 2 | 96 | `95` | EU chapter (same as WCO) |
| `EU_CN_4` | 4 | 1,228 | `9503` | EU heading (same as WCO) |
| `EU_CN_8` | 8 | 11,376 | `95030030` | EU Combined Nomenclature (EU-specific detail) |

The 4-digit `EU_CN_4` headings are the level at which regulations are linked. The 8-digit `EU_CN_8` codes are what users search for.

**Example rows:**

| code | code_type | description | parent_code | country_scope |
|------|-----------|-------------|-------------|---------------|
| `9503` | `EU_CN_4` | Tricycles, scooters, pedal cars and similar wheeled toys; dolls' carriages; dolls; other toys... | `95` | `EU` |
| `95030030` | `EU_CN_8` | Electric trains, including tracks, signals and other accessories therefor; reduced-size (scale) model assembly kits | `9503` | `EU` |

**How it's queried:** Full-text search (GIN index) on `description` filtered by `country_scope`. For Germany, the scope is `EU` so it searches `EU_CN_8` codes. The UI shows matching codes as clickable candidates.

---

### 2. `regulations` — EU Directives & Regulations (34 rows)

Each row is one EU directive or regulation relevant to product compliance.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `source_id` | int FK → sources | Where the data came from |
| `title` | text | Full title (e.g. "Directive 2009/48/EC — Toy Safety") |
| `document_type` | text | `directive` or `regulation` |
| `authority` | text | Always "European Commission" |
| `country` | text | `DE` (scoped to Germany for now) |
| `effective_date` | date | When the directive became applicable |
| `status` | text | `active` |
| `directive_ref` | text | Reference number (e.g. `2009/48/EC`, `2017/745`) — **the primary join key** |

**The `directive_ref` field is critical.** It's the string that connects regulations to harmonised standards and notified bodies. It appears in three tables: `regulations`, `kb_harmonised_standards`, and `kb_notified_body_directives`.

**Example row:**

| id | title | document_type | country | effective_date | directive_ref |
|----|-------|---------------|---------|----------------|---------------|
| 13 | Directive 2009/48/EC — Toy Safety | directive | DE | 2011-07-20 | 2009/48/EC |

---

### 3. `regulation_hs_codes` — The Bridge Table (371 links)

Links regulations to HS codes. This is where the system decides **which directives apply to which products**.

| Column | Type | Description |
|--------|------|-------------|
| `regulation_id` | int FK → regulations | Which regulation |
| `hs_code_id` | int FK → kb_hs_codes | Which HS heading (always a 4-digit `EU_CN_4` code) |
| `confidence` | float | 0.9 for curated mappings |
| `mapping_method` | text | `fallback_curated` — how the link was established |

**Example:** Toy Safety (regulation_id=13) is linked to three headings:

| regulation_id | hs_code_id | code | mapping_method |
|---------------|------------|------|----------------|
| 13 | 20166 | `9503` | fallback_curated |
| 13 | 20167 | `9504` | fallback_curated |
| 13 | 20168 | `9505` | fallback_curated |

**The prefix match query:**

```sql
SELECT DISTINCT r.*
FROM regulations r
JOIN regulation_hs_codes rhc ON r.id = rhc.regulation_id
JOIN kb_hs_codes hc ON rhc.hs_code_id = hc.id
WHERE hc.code = LEFT('95030030', LENGTH(hc.code))
```

This takes the user's 8-digit code `95030030`, truncates it to match each linked heading's length (4 digits → `9503`), and finds a match. Result: Toy Safety directive applies.

---

### 4. `kb_harmonised_standards` — EN Standards (3,895 rows)

EU harmonised standards that give "presumption of conformity" with a directive. If a product meets the relevant EN standard, it's presumed to meet the directive's essential requirements.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `standard_code` | text | EN standard reference (e.g. `EN 71-1:2014+A1:2018`) |
| `title` | text | Full title (e.g. "Safety of toys - Part 1: Mechanical and physical properties") |
| `eso` | text | European Standards Organisation — `CEN`, `Cenelec`, or `ETSI` |
| `directive_ref` | text | **Join key** — which directive this standard supports (e.g. `2009/48/EC`) |
| `in_force_from` | date | When the standard gained legal presumption |
| `withdrawn_on` | date | NULL if still in force, date if withdrawn |
| `source_url` | text | EU Commission harmonised standards portal |
| `confidence` | float | 0.92 (official EU portal, deterministic extraction) |
| `extraction_method` | text | `oj_xlsx` — parsed from Official Journal XLSX files |

**Unique constraint:** `(standard_code, directive_ref)` — the same standard can appear under multiple directives.

**How it's queried:** After regulations are found, their `directive_ref` values are collected. Standards are then fetched:

```sql
SELECT standard_code, title, eso, directive_ref, in_force_from
FROM kb_harmonised_standards
WHERE directive_ref = ANY('{2009/48/EC}')
  AND withdrawn_on IS NULL
```

Only in-force standards are returned (`withdrawn_on IS NULL`).

**Coverage:** 17 of 34 directives have standards loaded. Directives with standards:

| Directive | directive_ref | Standards | In Force |
|-----------|--------------|-----------|----------|
| Machinery | 2006/42/EC | 1,279 | 816 |
| LVD | 2014/35/EU | 918 | 573 |
| PED | 2014/68/EU | 290 | 203 |
| RED | 2014/53/EU | 263 | 182 |
| PPE | 2016/425 | 252 | 193 |
| Railway | 2016/797 | 215 | 127 |
| EMC | 2014/30/EU | 175 | 138 |
| ATEX | 2014/34/EU | 141 | 92 |
| Recreational Craft | 2013/53/EU | 135 | 62 |
| MDR | 2017/745 | 51 | 51 |
| Gas Appliances | 2016/426 | 36 | 21 |
| Toy Safety | 2009/48/EC | 34 | 11 |
| MID | 2014/32/EU | 30 | 13 |
| Lifts | 2014/33/EU | 28 | 14 |
| IVDR | 2017/746 | 23 | 23 |
| Cableway | 2016/424 | 23 | 16 |
| RoHS | 2011/65/EU | 2 | 1 |

---

### 5. `kb_notified_bodies` — Certification Bodies (186 rows)

Organisations authorised by Germany to perform conformity assessments under EU directives.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `nb_number` | text | EU-wide notified body number (e.g. `0044`) — unique across the EU |
| `name` | text | Organisation name (e.g. "TÜV NORD CERT GmbH") |
| `country_id` | int FK → kb_country_profiles | Always Germany (DE) currently |
| `city` | text | City and postal code |
| `email` | text | Contact email |
| `website` | text | Organisation website |
| `status` | text | `active` |
| `source_url` | text | NANDO (EU notified body database) |
| `confidence` | float | 0.90 (official EU portal) |

---

### 6. `kb_notified_body_directives` — NB Scope of Designation (324 rows)

Which directives each notified body is authorised to certify under. One body can be designated for multiple directives.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `nb_id` | int FK → kb_notified_bodies | Which body |
| `directive_ref` | text | **Join key** — which directive (e.g. `2009/48/EC`) |
| `directive_name` | text | Human-readable name (e.g. "Toys safety") |
| `notifying_authority` | text | German authority that approved them (e.g. `ZLS`) |
| `last_approval_date` | date | Most recent approval/renewal date |

**How it's queried:** Same pattern as standards — uses the `directive_ref` values from matched regulations:

```sql
SELECT nb.nb_number, nb.name, nb.city, nb.email, nb.website,
       nbd.directive_ref, nbd.notifying_authority, nbd.last_approval_date
FROM kb_notified_body_directives nbd
JOIN kb_notified_bodies nb ON nbd.nb_id = nb.id
JOIN kb_country_profiles cp ON nb.country_id = cp.id
WHERE cp.iso2 = 'DE'
  AND nbd.directive_ref = ANY('{2009/48/EC}')
  AND nb.status = 'active'
```

**Coverage:** 30 of 34 directives have notified bodies designated in Germany.

---

### 7. `sources` — Data Provenance (34 rows)

Tracks where each regulation's data was fetched from.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int PK | Internal ID |
| `source_name` | text | Source identifier (e.g. `eurlex_cellar`) |
| `document_id` | text | CELEX number (e.g. `32009L0048`) |
| `url` | text | Source URL |
| `fetched_at` | timestamp | When the data was last fetched |
| `content_hash` | text | Hash of fetched content for change detection |

Each regulation row has a `source_id` FK pointing here.

---

### 8. Country Context Tables

These provide background context about Germany's regulatory environment.

**`kb_country_profiles`** (1 row) — Germany's profile:

| iso2 | country_name | block_id |
|------|-------------|----------|
| DE | Germany | 1 (→ EU) |

**`kb_economic_blocks`** (1 row):

| code | name |
|------|------|
| EU | European Union |

**`kb_memberships`** (10 rows) — Germany's memberships in international standards and trade bodies:

| org_code | membership_type |
|----------|----------------|
| WTO | full |
| ISO | full |
| IEC | full |
| CEN | full |
| CENELEC | full |
| BIPM | full |
| ILAC | full |
| IAF | full |
| OIML | full |
| EU | full |

**`kb_regulatory_authorities`** (4 rows) — German market surveillance authorities:

| name | acronym | authority_type |
|------|---------|---------------|
| Bundesanstalt für Arbeitsschutz und Arbeitsmedizin | BAuA | market_surveillance |
| Bundesnetzagentur | BNetzA | market_surveillance |
| Bundesinstitut für Arzneimittel und Medizinprodukte | BfArM | market_surveillance |
| Zentralstelle der Länder für Sicherheitstechnik | ZLS | notifying_authority |

**`kb_laws`** (4 rows) — German national laws (metadata only, not linked to HS codes):

| title | law_type | scope |
|-------|----------|-------|
| Produktsicherheitsgesetz (ProdSG) | federal_act | Horizontal product safety framework |
| Elektro- und Elektronikgerätegesetz (ElektroG) | federal_act | WEEE transposition |
| EMVG | federal_act | EMC Directive transposition |
| MPDG | federal_act | MDR transposition |

**`kb_mras`** (2 rows) — Mutual Recognition Agreements relevant to Germany via EU membership.

---

## The Full Retrieval Sequence

When the API receives a request like `POST /hs-codes/compliance-by-code {hs_code: "95030030", country_code: "DE"}`, it executes this sequence:

### Step 1: Country Context

```sql
-- Get country profile and block
SELECT cp.iso2, cp.country_name, eb.code, eb.name
FROM kb_country_profiles cp
LEFT JOIN kb_economic_blocks eb ON cp.block_id = eb.id
WHERE cp.iso2 = 'DE'

-- Get memberships
SELECT org_code FROM kb_memberships
WHERE country_id = (SELECT id FROM kb_country_profiles WHERE iso2 = 'DE')
  AND is_member = TRUE
```

**Result:** `{iso2: "DE", country_name: "Germany", block_code: "EU", memberships: ["WTO","ISO","IEC",...]}`

### Step 2: Find Matching Regulations (Prefix Match)

```sql
SELECT DISTINCT r.id, r.title, r.document_type, r.authority,
       r.country, r.effective_date, r.status, r.directive_ref
FROM regulations r
JOIN regulation_hs_codes rhc ON r.id = rhc.regulation_id
JOIN kb_hs_codes hc ON rhc.hs_code_id = hc.id
WHERE r.country = 'DE'
  AND hc.code = LEFT('95030030', LENGTH(hc.code))
```

**How the prefix match works:**
- `kb_hs_codes` linked via `regulation_hs_codes` are 4-digit headings (e.g. `9503`)
- `LENGTH('9503')` = 4
- `LEFT('95030030', 4)` = `9503`
- `'9503' = '9503'` → **match**

**Result:** Regulation id=13 (Toy Safety, directive_ref=`2009/48/EC`)

### Step 3: Collect directive_ref Values

From the matched regulations, extract all unique `directive_ref` values:

```
directive_refs = ['2009/48/EC']
```

### Step 4: Fetch Notified Bodies

```sql
SELECT nb.nb_number, nb.name, nb.city, nb.email, nb.website,
       nbd.directive_ref, nbd.notifying_authority, nbd.last_approval_date
FROM kb_notified_body_directives nbd
JOIN kb_notified_bodies nb ON nbd.nb_id = nb.id
JOIN kb_country_profiles cp ON nb.country_id = cp.id
WHERE cp.iso2 = 'DE'
  AND nbd.directive_ref = ANY('{2009/48/EC}')
  AND nb.status = 'active'
```

**Result:** 10 German notified bodies designated for Toy Safety (TÜV NORD, TÜV SÜD, TÜV Rheinland, Intertek, Bureau Veritas, SGS, etc.)

### Step 5: Fetch Harmonised Standards

```sql
SELECT standard_code, title, eso, directive_ref, in_force_from
FROM kb_harmonised_standards
WHERE directive_ref = ANY('{2009/48/EC}')
  AND withdrawn_on IS NULL
```

**Result:** 11 in-force standards (EN 71-1, EN 71-2, EN 71-3, EN 71-4, EN 71-5, EN 71-7, EN 71-8, EN 71-12, EN 71-13, EN 71-14, EN IEC 62115)

### Step 6: Return Combined Response

All results are assembled into a single JSON response containing:
- Country context (profile, block, memberships)
- HS code metadata (code, description, code_type)
- Matching regulations (title, type, authority, effective date, status)
- Notified bodies (number, name, city, contact, directive scope, approval date)
- Harmonised standards (code, title, ESO, directive, in-force date)

---

## Relationship Diagram

```
kb_country_profiles ───┐
        │               │
        │ block_id      │ country_id
        ▼               │
kb_economic_blocks      ├──► kb_memberships
                        ├──► kb_laws
                        ├──► kb_regulatory_authorities
                        ├──► kb_mras
                        │
                        └──► kb_notified_bodies
                                    │
                                    │ nb_id
                                    ▼
                             kb_notified_body_directives
                                    │
                                    │ directive_ref (text match)
                                    │
           sources ◄── source_id ── regulations
                                    │
                                    │ regulation_id
                                    ▼
                             regulation_hs_codes
                                    │
                                    │ hs_code_id
                                    ▼
                               kb_hs_codes


           kb_harmonised_standards
                    │
                    │ directive_ref (text match)
                    │
                    └──── matches ────► regulations.directive_ref
```

**Key relationships:**

| From | To | Join Type |
|------|----|-----------|
| `regulations` → `regulation_hs_codes` → `kb_hs_codes` | Foreign keys | `regulation_id`, `hs_code_id` |
| `regulations` → `kb_harmonised_standards` | **Text match** on `directive_ref` | No FK — matched by string |
| `regulations` → `kb_notified_body_directives` | **Text match** on `directive_ref` | No FK — matched by string |
| `kb_notified_body_directives` → `kb_notified_bodies` | Foreign key | `nb_id` |
| `kb_notified_bodies` → `kb_country_profiles` | Foreign key | `country_id` |
| `regulations` → `sources` | Foreign key | `source_id` |

**Important:** Harmonised standards and notified body directives connect to regulations through the `directive_ref` **text field**, not through a foreign key. This means:
- The string must match exactly (e.g. `2016/797` ≠ `2016/797/EU`)
- There is no referential integrity enforced by the database for these joins
- Adding a new regulation with a `directive_ref` automatically connects it to any existing standards and notified bodies with the same ref

---

## Data Sources

| Table | Source | Source Type |
|-------|--------|-------------|
| `kb_hs_codes` | WCO / EU Combined Nomenclature | Official tariff databases |
| `regulations` | EUR-Lex CELLAR repository | EU open data (publications.europa.eu) |
| `regulation_hs_codes` | Curated mapping | Hand-curated 4-digit heading assignments per directive |
| `kb_harmonised_standards` | EU Commission harmonised standards portal | Official XLSX summary lists |
| `kb_notified_bodies` | NANDO database | EU notified body registry |
| `kb_notified_body_directives` | NANDO database | EU notified body registry |
| `kb_country_profiles` | Seed data | Manual entry |
| `kb_memberships` | Seed data | Manual entry |

---

## Known Limitations

1. **HS mapping granularity:** Regulations are linked at the 4-digit heading level. Two products under the same heading (e.g. a table fan and an ATEX-rated industrial fan, both under 8414) get the same directives — the system cannot distinguish them without reading the product description.

2. **directive_ref text matching:** Standards and notified bodies are linked to regulations by a text string, not a foreign key. A typo or format inconsistency (e.g. `2016/797` vs `2016/797/EU`) breaks the join silently.

3. **Standards coverage:** Only 17 of 34 directives have harmonised standards loaded. The remaining 17 directives have no standards data because the EU Commission does not publish XLSX files for them.

4. **Country scope:** Only Germany (DE) is loaded. The EU directives, standards, and HS codes are EU-wide, but notified bodies are filtered to German ones only.

5. **No chemical/environmental regulations:** REACH, CLP, WEEE, packaging, and battery regulations are not in the system. A product can be CE-marked correctly but still non-compliant on substance restrictions.
