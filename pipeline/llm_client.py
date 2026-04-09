"""Centralised LLM client — all Anthropic API calls go through this module."""

import json
import logging
import os

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-6"
EMBEDDING_MODEL = "voyage-3"
MAX_RETRIES = 3


class LLMClient:
    """Thin wrapper around the Anthropic SDK used by both pipeline and KB."""

    def __init__(self) -> None:
        api_key = os.environ["ANTHROPIC_API_KEY"]
        self.client = anthropic.Anthropic(api_key=api_key)

    # ------------------------------------------------------------------
    # Structured extraction
    # ------------------------------------------------------------------
    def extract_structured(
        self,
        text: str,
        schema: dict,
        system_context: str = "",
    ) -> dict:
        """Call Opus to extract fields matching *schema* from *text*.

        Retries up to MAX_RETRIES times on JSON parse failures.
        """
        user_prompt = (
            "Extract the following fields. Return ONLY valid JSON matching "
            "this schema. No preamble, no markdown fences.\n\n"
            f"Schema:\n{json.dumps(schema, indent=2)}\n\nText:\n{text}"
        )

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system_context,
                messages=[{"role": "user", "content": user_prompt}],
            )
            raw = response.content[0].text
            logger.debug(
                "extract_structured  model=%s  input_tokens=%s  output_tokens=%s",
                response.model,
                response.usage.input_tokens,
                response.usage.output_tokens,
            )
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                last_error = exc
                logger.debug(
                    "JSON parse failed (attempt %d/%d): %s",
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
        raise last_error  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Plain-text summary
    # ------------------------------------------------------------------
    def generate_summary(self, text: str, max_words: int = 150) -> str:
        """Return a plain-text summary of *text* in at most *max_words* words."""
        response = self.client.messages.create(
            model=MODEL,
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Summarise the following text in at most {max_words} words. "
                        "Return only the summary, no preamble.\n\n"
                        f"{text}"
                    ),
                }
            ],
        )
        logger.debug(
            "generate_summary  model=%s  input_tokens=%s  output_tokens=%s",
            response.model,
            response.usage.input_tokens,
            response.usage.output_tokens,
        )
        return response.content[0].text

    # ------------------------------------------------------------------
    # HS-code classification
    # ------------------------------------------------------------------
    def classify_hs_code(
        self,
        product_description: str,
        candidates: list[dict],
    ) -> dict:
        """Pick the best HS code from *candidates* for *product_description*.

        Returns dict with keys:
            code, code_type, confidence (0.0-1.0), reasoning, national_variant
        """
        response = self.client.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=(
                "You are an HS-code classification expert. Given a product "
                "description and candidate HS codes, select the single best "
                "match. Return ONLY valid JSON with these keys: "
                "code, code_type, confidence (float 0.0-1.0), reasoning, "
                "national_variant (string or null)."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Product description:\n{product_description}\n\n"
                        f"Candidate codes:\n{json.dumps(candidates, indent=2)}\n\n"
                        "Select the best code. Return ONLY valid JSON."
                    ),
                }
            ],
        )
        raw = response.content[0].text
        logger.debug(
            "classify_hs_code  model=%s  input_tokens=%s  output_tokens=%s",
            response.model,
            response.usage.input_tokens,
            response.usage.output_tokens,
        )
        return json.loads(raw)

    # ------------------------------------------------------------------
    # Embeddings
    # ------------------------------------------------------------------
    def embed_text(self, text: str) -> list[float]:
        """Return a 1536-dim embedding vector for *text*."""
        response = self.client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=[text],
        )
        embedding = response.data[0].embedding
        logger.debug(
            "embed_text  model=%s  input_tokens=%s",
            EMBEDDING_MODEL,
            response.usage.total_tokens,
        )
        return embedding
