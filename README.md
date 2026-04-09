# Trade Compliance

Given a destination country and product type, returns all relevant regulations, required standards, HS codes, and practical compliance requirements.

## Architecture

- **Knowledge Base (KB)** — country/block profiles, standards acceptance, HS code library, memberships, MRAs
- **Regulation Pipeline** — ingests regulations from EUR-Lex, Federal Register, etc., classifies HS codes via RAG, queues low-confidence items for review
- **API** — FastAPI query layer over the shared PostgreSQL database

## Stack

Python 3.11 · PostgreSQL 16 + pgvector · Prefect · FastAPI · Anthropic Claude

## Quick start

```bash
cp .env.example .env          # fill in ANTHROPIC_API_KEY and DATABASE_URL
pip install -r requirements.txt
bash db/init.sh               # creates 17 tables
uvicorn api.main:app --reload # http://localhost:8000/health
```
