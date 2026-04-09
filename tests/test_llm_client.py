"""Tests for pipeline.llm_client — all Anthropic calls are mocked."""

import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Ensure env var is set before import
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

from pipeline.llm_client import LLMClient, MAX_RETRIES, MODEL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_usage(input_tokens=10, output_tokens=20):
    return SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens)


def _make_message(text, model=MODEL, input_tokens=10, output_tokens=20):
    """Build a minimal object that looks like an Anthropic Messages response."""
    return SimpleNamespace(
        content=[SimpleNamespace(text=text)],
        model=model,
        usage=_make_usage(input_tokens, output_tokens),
    )


def _make_embedding(vector, total_tokens=5):
    return SimpleNamespace(
        data=[SimpleNamespace(embedding=vector)],
        usage=SimpleNamespace(total_tokens=total_tokens),
    )


@pytest.fixture()
def client():
    with patch("pipeline.llm_client.anthropic.Anthropic"):
        c = LLMClient()
    return c


# ---------------------------------------------------------------------------
# extract_structured
# ---------------------------------------------------------------------------

class TestExtractStructured:

    def test_returns_parsed_json(self, client):
        payload = {"name": "Widget", "weight_kg": 1.5}
        client.client.messages.create.return_value = _make_message(
            json.dumps(payload)
        )
        schema = {"name": "string", "weight_kg": "number"}
        result = client.extract_structured("some text", schema)
        assert result == payload

    def test_passes_system_context(self, client):
        client.client.messages.create.return_value = _make_message('{"a": 1}')
        client.extract_structured("txt", {"a": "int"}, system_context="be precise")
        call_kwargs = client.client.messages.create.call_args.kwargs
        assert call_kwargs["system"] == "be precise"

    def test_retries_on_bad_json(self, client):
        good = json.dumps({"ok": True})
        client.client.messages.create.side_effect = [
            _make_message("not json!!!"),
            _make_message(good),
        ]
        result = client.extract_structured("txt", {"ok": "bool"})
        assert result == {"ok": True}
        assert client.client.messages.create.call_count == 2

    def test_raises_after_max_retries(self, client):
        client.client.messages.create.return_value = _make_message("bad json")
        with pytest.raises(json.JSONDecodeError):
            client.extract_structured("txt", {"x": "int"})
        assert client.client.messages.create.call_count == MAX_RETRIES

    def test_uses_correct_model(self, client):
        client.client.messages.create.return_value = _make_message('{"a":1}')
        client.extract_structured("t", {"a": "int"})
        assert client.client.messages.create.call_args.kwargs["model"] == MODEL


# ---------------------------------------------------------------------------
# generate_summary
# ---------------------------------------------------------------------------

class TestGenerateSummary:

    def test_returns_plain_text(self, client):
        client.client.messages.create.return_value = _make_message("Short summary.")
        result = client.generate_summary("long text " * 100)
        assert result == "Short summary."

    def test_max_words_in_prompt(self, client):
        client.client.messages.create.return_value = _make_message("ok")
        client.generate_summary("text", max_words=50)
        prompt = client.client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "50 words" in prompt

    def test_uses_correct_model(self, client):
        client.client.messages.create.return_value = _make_message("ok")
        client.generate_summary("text")
        assert client.client.messages.create.call_args.kwargs["model"] == MODEL


# ---------------------------------------------------------------------------
# classify_hs_code
# ---------------------------------------------------------------------------

class TestClassifyHsCode:

    def test_returns_classification(self, client):
        expected = {
            "code": "8471.30",
            "code_type": "subheading",
            "confidence": 0.92,
            "reasoning": "Portable digital computer",
            "national_variant": None,
        }
        client.client.messages.create.return_value = _make_message(
            json.dumps(expected)
        )
        candidates = [
            {"code": "8471.30", "description": "Portable digital computers"},
            {"code": "8471.41", "description": "Other computers"},
        ]
        result = client.classify_hs_code("laptop computer", candidates)
        assert result == expected

    def test_candidates_in_prompt(self, client):
        client.client.messages.create.return_value = _make_message(
            '{"code":"0","code_type":"","confidence":0,"reasoning":"","national_variant":null}'
        )
        candidates = [{"code": "1234.56", "description": "test"}]
        client.classify_hs_code("desc", candidates)
        prompt = client.client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "1234.56" in prompt

    def test_uses_correct_model(self, client):
        client.client.messages.create.return_value = _make_message(
            '{"code":"0","code_type":"","confidence":0,"reasoning":"","national_variant":null}'
        )
        client.classify_hs_code("x", [])
        assert client.client.messages.create.call_args.kwargs["model"] == MODEL


# ---------------------------------------------------------------------------
# embed_text
# ---------------------------------------------------------------------------

class TestEmbedText:

    def test_returns_vector(self, client):
        vec = [0.1] * 1536
        client.client.embeddings.create.return_value = _make_embedding(vec)
        result = client.embed_text("hello world")
        assert result == vec
        assert len(result) == 1536

    def test_passes_input_as_list(self, client):
        client.client.embeddings.create.return_value = _make_embedding([0.0])
        client.embed_text("test")
        call_kwargs = client.client.embeddings.create.call_args.kwargs
        assert call_kwargs["input"] == ["test"]
