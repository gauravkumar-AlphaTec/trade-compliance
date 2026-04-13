"""Centralised LLM client — all inference calls go through this module.

Uses Ollama (local) with gemma4:e4b for all LLM work.
"""

import json
import logging
import os

import httpx

logger = logging.getLogger(__name__)

MODEL = "gemma4:e4b"
MAX_RETRIES = 3
TIMEOUT = 120


class LLMClient:
    """Thin wrapper around the Ollama chat API used by both pipeline and KB."""

    def __init__(self) -> None:
        self.base_url = os.environ.get("OLLAMA_HOST", "http://ollama:11434")

    def _call(self, messages: list[dict]) -> str:
        """Send a chat completion request to Ollama and return the response text."""
        payload = {
            "model": MODEL,
            "messages": messages,
            "stream": False,
        }
        response = httpx.post(
            f"{self.base_url}/api/chat",
            json=payload,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        content = data["message"]["content"]

        char_count = sum(len(m.get("content", "")) for m in messages)
        logger.debug(
            "ollama call  model=%s  input_chars=%d  output_chars=%d",
            MODEL,
            char_count,
            len(content),
        )
        return content

    # ------------------------------------------------------------------
    # Structured extraction
    # ------------------------------------------------------------------
    def extract_structured(
        self,
        text: str,
        schema: dict,
        system_context: str = "",
    ) -> dict:
        """Call Ollama to extract fields matching *schema* from *text*.

        Retries up to MAX_RETRIES times on JSON parse failures.
        """
        messages = []
        if system_context:
            messages.append({"role": "system", "content": system_context})
        messages.append({
            "role": "user",
            "content": (
                "Extract the following fields. Return ONLY valid JSON matching "
                "this schema. No preamble, no markdown fences.\n\n"
                f"Schema:\n{json.dumps(schema, indent=2)}\n\nText:\n{text}"
            ),
        })

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            raw = self._call(messages)
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
        messages = [
            {
                "role": "user",
                "content": (
                    f"Summarise the following text in at most {max_words} words. "
                    "Return only the summary, no preamble.\n\n"
                    f"{text}"
                ),
            }
        ]
        return self._call(messages)

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

        Confidence is scaled by 0.85 to account for local model accuracy.
        """
        messages = [
            {
                "role": "system",
                "content": (
                    "You are an HS-code classification expert. Given a product "
                    "description and candidate HS codes, select the single best "
                    "match. Return ONLY valid JSON with these keys: "
                    "code, code_type, confidence (float 0.0-1.0), reasoning, "
                    "national_variant (string or null)."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Product description:\n{product_description}\n\n"
                    f"Candidate codes:\n{json.dumps(candidates, indent=2)}\n\n"
                    "Select the best code. Return ONLY valid JSON."
                ),
            },
        ]
        raw = self._call(messages)
        result = json.loads(raw)
        result["confidence"] = round(result["confidence"] * 0.85, 3)
        return result

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check that Ollama is running and gemma4:e4b is available.

        Returns True if the model is found, False otherwise.
        """
        try:
            response = httpx.get(
                f"{self.base_url}/api/tags",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            models = [m.get("name", "") for m in data.get("models", [])]
            found = any(MODEL in name for name in models)
            if not found:
                logger.warning(
                    "Model %s not found in Ollama. Available: %s",
                    MODEL,
                    models,
                )
            return found
        except httpx.HTTPError as exc:
            logger.warning("Ollama health check failed: %s", exc)
            return False
