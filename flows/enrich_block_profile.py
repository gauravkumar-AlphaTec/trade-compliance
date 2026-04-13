"""Prefect flow: enrich EU block profile in kb_economic_blocks / kb_block_profiles."""

import json
import logging
import os

import psycopg2
from prefect import flow, task

from kb.sources.eu_block import build_eu_block_profile

logger = logging.getLogger(__name__)


@task
def fetch_profile() -> dict:
    """Build the EU block profile from EC sources."""
    return build_eu_block_profile()


@task
def upsert_eu_block(profile: dict) -> None:
    """Upsert the EU block and its profile into the database."""
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    cur = conn.cursor()

    try:
        # Upsert kb_economic_blocks
        cur.execute(
            """
            INSERT INTO kb_economic_blocks (code, name, block_type, official_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                block_type = EXCLUDED.block_type,
                official_url = EXCLUDED.official_url
            RETURNING id
            """,
            ("EU", "European Union", "customs_union", profile.get("source_url", "")),
        )
        block_id = cur.fetchone()[0]

        # Upsert kb_block_profiles
        cur.execute(
            """
            INSERT INTO kb_block_profiles
                (block_id, directives, harmonized_standards, shared_mras,
                 conformity_framework, last_updated)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (block_id) DO UPDATE SET
                directives = EXCLUDED.directives,
                harmonized_standards = EXCLUDED.harmonized_standards,
                shared_mras = EXCLUDED.shared_mras,
                conformity_framework = EXCLUDED.conformity_framework,
                last_updated = NOW()
            """,
            (
                block_id,
                json.dumps(profile["directives"]),
                json.dumps(profile["harmonized_standards"]),
                json.dumps(profile["shared_mras"]),
                json.dumps(profile["conformity_framework"]),
            ),
        )
    finally:
        cur.close()
        conn.close()


@flow(name="enrich-eu-block")
def enrich_eu_block() -> None:
    """Fetch EU directives + harmonized standards and upsert into KB.

    Schedule: monthly (EU directives change rarely).
    """
    profile = fetch_profile()

    n_directives = len(profile.get("directives", []))
    n_standards = len(profile.get("harmonized_standards", []))
    logger.info(
        "EU block profile built: %d directives, %d harmonized standards",
        n_directives,
        n_standards,
    )

    upsert_eu_block(profile)

    logger.info("EU block profile upserted successfully")


if __name__ == "__main__":
    enrich_eu_block()
