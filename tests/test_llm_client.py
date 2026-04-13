"""Tests for pipeline.llm_client — all Ollama HTTP calls are mocked."""

import json
import os
from unittest.mock import MagicMock, patch

import httpx
import pytest

from pipeline.llm_client import LLMClient, MAX_RETRIES, MODEL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ollama_response(content: str, status_code: int = 200):
    """Build a mock httpx.Response matching Ollama's chat API shape."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = {"message": {"content": content}}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


def _tags_response(model_names: list[str]):
    """Build a mock response for GET /api/tags."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = {
        "models": [{"name": n} for n in model_names]
    }
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture()
def client():
    os.environ["OLLAMA_HOST"] = "http://test-ollama:11434"
    return LLMClient()


# ---------------------------------------------------------------------------
# _call
# ---------------------------------------------------------------------------

class TestCall:

    @patch("pipeline.llm_client.httpx.post")
    def test_sends_correct_payload(self, mock_post, client):
        mock_post.return_value = _ollama_response("hello")
        client._call([{"role": "user", "content": "hi"}])

        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["json"]["model"] == MODEL
        assert call_kwargs["json"]["stream"] is False
        assert call_kwargs["json"]["messages"] == [{"role": "user", "content": "hi"}]

    @patch("pipeline.llm_client.httpx.post")
    def test_returns_content(self, mock_post, client):
        mock_post.return_value = _ollama_response("world")
        result = client._call([{"role": "user", "content": "hi"}])
        assert result == "world"

    @patch("pipeline.llm_client.httpx.post")
    def test_uses_240s_timeout(self, mock_post, client):
        mock_post.return_value = _ollama_response("ok")
        client._call([{"role": "user", "content": "hi"}])
        assert mock_post.call_args.kwargs["timeout"] == 240


# ---------------------------------------------------------------------------
# extract_structured
# ---------------------------------------------------------------------------

class TestExtractStructured:

    @patch("pipeline.llm_client.httpx.post")
    def test_returns_parsed_json(self, mock_post, client):
        payload = {"name": "Widget", "weight_kg": 1.5}
        mock_post.return_value = _ollama_response(json.dumps(payload))
        schema = {"name": "string", "weight_kg": "number"}
        result = client.extract_structured("some text", schema)
        assert result == payload

    @patch("pipeline.llm_client.httpx.post")
    def test_passes_system_context(self, mock_post, client):
        mock_post.return_value = _ollama_response('{"a": 1}')
        client.extract_structured("txt", {"a": "int"}, system_context="be precise")
        sent = mock_post.call_args.kwargs["json"]["messages"]
        assert sent[0]["role"] == "system"
        assert sent[0]["content"] == "be precise"

    @patch("pipeline.llm_client.httpx.post")
    def test_no_system_message_when_empty(self, mock_post, client):
        mock_post.return_value = _ollama_response('{"a": 1}')
        client.extract_structured("txt", {"a": "int"})
        sent = mock_post.call_args.kwargs["json"]["messages"]
        assert all(m["role"] != "system" for m in sent)

    @patch("pipeline.llm_client.httpx.post")
    def test_retries_on_bad_json(self, mock_post, client):
        mock_post.side_effect = [
            _ollama_response("not json!!!"),
            _ollama_response(json.dumps({"ok": True})),
        ]
        result = client.extract_structured("txt", {"ok": "bool"})
        assert result == {"ok": True}
        assert mock_post.call_count == 2

    @patch("pipeline.llm_client.httpx.post")
    def test_raises_after_max_retries(self, mock_post, client):
        mock_post.return_value = _ollama_response("bad json")
        with pytest.raises(json.JSONDecodeError):
            client.extract_structured("txt", {"x": "int"})
        assert mock_post.call_count == MAX_RETRIES


# ---------------------------------------------------------------------------
# generate_summary
# ---------------------------------------------------------------------------

class TestGenerateSummary:

    @patch("pipeline.llm_client.httpx.post")
    def test_returns_plain_text(self, mock_post, client):
        mock_post.return_value = _ollama_response("Short summary.")
        result = client.generate_summary("long text " * 100)
        assert result == "Short summary."

    @patch("pipeline.llm_client.httpx.post")
    def test_max_words_in_prompt(self, mock_post, client):
        mock_post.return_value = _ollama_response("ok")
        client.generate_summary("text", max_words=50)
        sent = mock_post.call_args.kwargs["json"]["messages"]
        user_msg = next(m for m in sent if m["role"] == "user")
        assert "50 words" in user_msg["content"]


# ---------------------------------------------------------------------------
# classify_hs_code
# ---------------------------------------------------------------------------

class TestClassifyHsCode:

    @patch("pipeline.llm_client.httpx.post")
    def test_returns_classification_with_scaled_confidence(self, mock_post, client):
        raw = {
            "code": "8471.30",
            "code_type": "subheading",
            "confidence": 0.92,
            "reasoning": "Portable digital computer",
            "national_variant": None,
        }
        mock_post.return_value = _ollama_response(json.dumps(raw))
        candidates = [
            {"code": "8471.30", "description": "Portable digital computers"},
        ]
        result = client.classify_hs_code("laptop computer", candidates)
        assert result["code"] == "8471.30"
        # 0.92 * 0.85 = 0.782
        assert result["confidence"] == 0.782

    @patch("pipeline.llm_client.httpx.post")
    def test_confidence_scaling_precision(self, mock_post, client):
        raw = {
            "code": "0",
            "code_type": "",
            "confidence": 1.0,
            "reasoning": "",
            "national_variant": None,
        }
        mock_post.return_value = _ollama_response(json.dumps(raw))
        result = client.classify_hs_code("x", [])
        assert result["confidence"] == 0.85

    @patch("pipeline.llm_client.httpx.post")
    def test_candidates_in_prompt(self, mock_post, client):
        raw = '{"code":"0","code_type":"","confidence":0,"reasoning":"","national_variant":null}'
        mock_post.return_value = _ollama_response(raw)
        candidates = [{"code": "1234.56", "description": "test"}]
        client.classify_hs_code("desc", candidates)
        sent = mock_post.call_args.kwargs["json"]["messages"]
        user_msg = next(m for m in sent if m["role"] == "user")
        assert "1234.56" in user_msg["content"]


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:

    @patch("pipeline.llm_client.httpx.get")
    def test_returns_true_when_model_found(self, mock_get, client):
        mock_get.return_value = _tags_response(["gemma4:e2b", "llama3:8b"])
        assert client.health_check() is True

    @patch("pipeline.llm_client.httpx.get")
    def test_returns_false_when_model_missing(self, mock_get, client):
        mock_get.return_value = _tags_response(["llama3:8b"])
        assert client.health_check() is False

    @patch("pipeline.llm_client.httpx.get")
    def test_returns_false_on_connection_error(self, mock_get, client):
        mock_get.side_effect = httpx.ConnectError("refused")
        assert client.health_check() is False

    @patch("pipeline.llm_client.httpx.get")
    def test_returns_true_for_partial_name_match(self, mock_get, client):
        mock_get.return_value = _tags_response(["gemma4:e2b-latest"])
        assert client.health_check() is True
