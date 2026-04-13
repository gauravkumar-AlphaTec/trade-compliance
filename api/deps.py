"""Shared dependencies for API routes: database connection and LLM client."""

import os

import psycopg2
from psycopg2.extras import RealDictCursor

from pipeline.llm_client import LLMClient

# Populated at app startup via lifespan
llm_client: LLMClient | None = None


def get_db():
    """Yield a database connection, closing it after the request."""
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )
    try:
        yield conn
    finally:
        conn.close()


def get_llm() -> LLMClient:
    """Return the app-wide LLMClient instance."""
    if llm_client is None:
        raise RuntimeError("LLMClient not initialised — check app lifespan")
    return llm_client
