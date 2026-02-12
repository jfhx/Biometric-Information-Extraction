import json
import logging
from typing import Any, Dict, Optional

import requests

from app.core.config import settings
from app.services.prompt_templates import EXTRACTION_SYSTEM_PROMPT

logger = logging.getLogger(__name__)


class LLMRequestError(RuntimeError):
    pass


def _build_qwen_payload(prompt: str) -> Dict[str, Any]:
    return {
        "model": settings.LLM_MODEL,
        "messages": [
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": settings.LLM_TEMPERATURE,
        "top_p": settings.LLM_TOP_P,
        "max_tokens": settings.LLM_MAX_TOKENS,
    }


def _resolve_chat_completions_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


def call_llm(prompt: str) -> str:
    provider = settings.LLM_PROVIDER.lower()
    if provider in {"qwen", "deepseek", "openai"}:
        # All these providers expose OpenAI-compatible chat completions in this project.
        url = _resolve_chat_completions_url(settings.LLM_BASE_URL)
        payload = _build_qwen_payload(prompt)
        headers: Dict[str, str] = {}
        if settings.LLM_API_KEY:
            headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    else:
        raise LLMRequestError(f"Unsupported LLM_PROVIDER: {settings.LLM_PROVIDER}")

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=settings.LLM_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.exception("LLM request failed")
        raise LLMRequestError(str(exc)) from exc

    if response.status_code >= 400:
        raise LLMRequestError(f"LLM error {response.status_code}: {response.text}")

    data: Dict[str, Any] = response.json()
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        logger.error("Unexpected LLM response: %s", json.dumps(data, ensure_ascii=False))
        raise LLMRequestError("Unexpected LLM response structure") from exc


def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
    if text.endswith("```"):
        text = text.rsplit("\n", 1)[0]
    return text.strip()


def parse_llm_json(text: str) -> Dict[str, Any]:
    cleaned = _strip_code_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Failed to parse JSON, returning raw text")
        return {"records": [], "raw_summary": cleaned}


def update_model_config(
    temperature: Optional[float] = None,
    top_p: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> None:
    if temperature is not None:
        settings.LLM_TEMPERATURE = float(temperature)
    if top_p is not None:
        settings.LLM_TOP_P = float(top_p)
    if max_tokens is not None:
        settings.LLM_MAX_TOKENS = int(max_tokens)
