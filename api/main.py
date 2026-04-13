"""Trade Compliance API.

FastAPI app with lifespan for LLMClient initialisation.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

import api.deps as deps
from api.routes.regulations import router as regulations_router
from api.routes.countries import router as countries_router
from api.routes.hs_codes import router as hs_codes_router
from pipeline.llm_client import LLMClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise LLMClient once at startup, tear down on shutdown."""
    deps.llm_client = LLMClient()
    yield
    deps.llm_client = None


app = FastAPI(
    title="Trade Compliance API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(regulations_router)
app.include_router(countries_router)
app.include_router(hs_codes_router)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}
