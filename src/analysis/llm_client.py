import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from src.analysis.prompts import (
    ANALYSIS_PROMPT_TEMPLATE,
    ANALYSIS_SYSTEM_PROMPT,
    FILTER_PROMPT_TEMPLATE,
    FILTER_SYSTEM_PROMPT,
)
from src.config import Settings, get_settings
from src.pipeline.storage import ArticleRecord, MarketDataRecord

logger = structlog.get_logger()


@dataclass
class FilterResult:
    relevance: int
    sentiment: str
    tickers: list[str]
    key_facts: list[str]


async def _call_ollama(
    model: str,
    prompt: str,
    system: str,
    temperature: float,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    url = f"{settings.ollama_base_url}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "system": system,
        "format": "json",
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": 2048,
        },
    }

    last_error: Exception | None = None
    for attempt in range(settings.ollama_max_retries):
        try:
            async with httpx.AsyncClient(timeout=settings.ollama_timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                raw = data.get("response", "{}")
                parsed: dict[str, Any] = json.loads(raw)
                return parsed
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            last_error = exc
            logger.warning(
                "ollama_retry",
                model=model,
                attempt=attempt + 1,
                error=str(exc),
            )
    logger.error("ollama_failed", model=model, error=str(last_error))
    return {}


async def check_ollama_health(settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    url = f"{settings.ollama_base_url}/api/tags"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            models = [m.get("name", "") for m in data.get("models", [])]
            logger.info("ollama_health_ok", models=models)
            return len(models) > 0
    except Exception:
        logger.exception("ollama_health_check_failed")
        return False


async def filter_article(article: ArticleRecord, settings: Settings | None = None) -> FilterResult:
    settings = settings or get_settings()
    prompt = FILTER_PROMPT_TEMPLATE.format(title=article.title, content=article.content[:500])
    result = await _call_ollama(
        model=settings.ollama_filter_model,
        prompt=prompt,
        system=FILTER_SYSTEM_PROMPT,
        temperature=settings.ollama_filter_temperature,
        settings=settings,
    )
    raw_tickers = result.get("tickers", [])
    raw_facts = result.get("key_facts", [])
    tickers_list: list[str] = [str(t) for t in raw_tickers] if isinstance(raw_tickers, list) else []
    facts_list: list[str] = [str(f) for f in raw_facts] if isinstance(raw_facts, list) else []
    return FilterResult(
        relevance=int(result.get("relevance", 0) or 0),
        sentiment=str(result.get("sentiment", "neutral")),
        tickers=tickers_list,
        key_facts=facts_list,
    )


def _format_market_data(records: list[MarketDataRecord]) -> str:
    lines: list[str] = []
    for r in records:
        lines.append(
            f"- {r.ticker} ({r.name}): ${r.price:.2f}, "
            f"1w: {r.change_1w_pct:+.1f}%, 1m: {r.change_1m_pct:+.1f}%"
        )
    return "\n".join(lines) if lines else "No market data available."


def _format_articles(records: list[ArticleRecord]) -> str:
    lines: list[str] = []
    for r in records:
        sentiment = r.sentiment or "unknown"
        lines.append(f"- [{sentiment}] {r.title}: {r.content[:200]}")
    return "\n".join(lines) if lines else "No articles available."


async def analyze_dataset(
    articles: list[ArticleRecord],
    market_data: list[MarketDataRecord],
    history_summary: str = "No previous recommendations.",
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    prompt = ANALYSIS_PROMPT_TEMPLATE.format(
        market_data=_format_market_data(market_data),
        articles=_format_articles(articles),
        history=history_summary,
        date=datetime.now(tz=UTC).strftime("%Y-%m-%d"),
        sources_count=len(articles),
    )
    result = await _call_ollama(
        model=settings.ollama_analysis_model,
        prompt=prompt,
        system=ANALYSIS_SYSTEM_PROMPT,
        temperature=settings.ollama_analysis_temperature,
        settings=settings,
    )
    return result
