"""Centralised LLM client — all inference calls go through this module.

Uses Ollama (local) with gemma4:e2b for all LLM work
(smaller variant chosen for hardware-limited deployments).
"""

import json
import logging
import os

import httpx

logger = logging.getLogger(__name__)

MODEL = "gemma4:e2b"
MAX_RETRIES = 3
TIMEOUT = 240


def _extract_json(raw: str) -> dict:
    """Parse JSON from LLM output, tolerating markdown fences and preamble.

    Smaller models (e.g. gemma4:e2b) often wrap JSON in ```json ... ``` fences
    or add a sentence before/after. Strip wrappers and parse the first
    balanced {...} object found.
    """
    text = raw.strip()

    # Strip ```json ... ``` or ``` ... ``` fences
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
        text = text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Find the first balanced {...} object
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                return json.loads(text[start:i + 1])

    raise json.JSONDecodeError("No JSON object found in LLM output", text, 0)


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
                return _extract_json(raw)
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
        result = _extract_json(raw)
        result["confidence"] = round(result["confidence"] * 0.85, 3)
        return result

    # ------------------------------------------------------------------
    # Directive scope → HS headings
    # ------------------------------------------------------------------
    def map_directive_to_hs_headings(
        self,
        directive_ref: str,
        scope_text: str,
        definitions_text: str = "",
    ) -> dict:
        """Given a directive's scope and definitions text, return HS headings.

        Returns dict with keys:
            hs_headings: list of 4-digit HS heading strings,
            reasoning: explanation of the mapping,
            confidence: float 0.0-1.0
        """
        combined = scope_text
        if definitions_text:
            combined += "\n\nDefinitions:\n" + definitions_text

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an EU trade-compliance expert. Given the scope and "
                    "definitions text of an EU directive, identify the EU Combined "
                    "Nomenclature (CN) 4-digit headings of products that fall "
                    "under this directive.\n\n"
                    "Rules:\n"
                    "- Return ONLY 4-digit HS/CN heading codes (e.g. 8501, 9503)\n"
                    "- Include headings for products IN scope\n"
                    "- Do NOT include headings for products explicitly EXCLUDED\n"
                    "- Be specific: prefer 4-digit headings over 2-digit chapters\n"
                    "- Return ONLY valid JSON"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Directive: {directive_ref}\n\n"
                    f"Scope and definitions text:\n{combined[:4000]}\n\n"
                    "Return JSON with keys: hs_headings (list of 4-digit strings), "
                    "reasoning (string), confidence (float 0.0-1.0)."
                ),
            },
        ]

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            raw = self._call(messages)
            try:
                result = _extract_json(raw)
                headings = result.get("hs_headings", [])
                result["hs_headings"] = [
                    str(h).strip()[:4] for h in headings if str(h).strip()
                ]
                result["confidence"] = round(
                    result.get("confidence", 0.5) * 0.85, 3
                )
                return result
            except json.JSONDecodeError as exc:
                last_error = exc
                logger.debug(
                    "JSON parse failed (attempt %d/%d): %s",
                    attempt, MAX_RETRIES, exc,
                )
        raise last_error  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check that Ollama is running and the configured model is available.

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
