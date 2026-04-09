"""Trade Compliance API."""

from fastapi import FastAPI

app = FastAPI(title="Trade Compliance API", version="0.1.0")


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}
