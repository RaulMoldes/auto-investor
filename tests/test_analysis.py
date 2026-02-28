import json

import httpx
import pytest
import respx

from src.analysis.llm_client import (
    FilterResult,
    _call_ollama,
    _format_articles,
    _format_market_data,
    check_ollama_health,
    filter_article,
)
from src.analysis.prompts import ANALYSIS_PROMPT_TEMPLATE, FILTER_PROMPT_TEMPLATE
from src.config import Settings
from src.pipeline.storage import ArticleRecord, MarketDataRecord


def _test_settings() -> Settings:
    return Settings(
        ollama_host="localhost",
        ollama_port=11434,
        ollama_filter_model="phi3:mini",
        ollama_analysis_model="mistral:7b",
        ollama_max_retries=1,
        ollama_timeout=5.0,
        sqlite_db_path=":memory:",
    )


@respx.mock
@pytest.mark.asyncio
async def test_call_ollama_success() -> None:
    response_body = {
        "response": json.dumps({"relevance": 8, "sentiment": "bullish"}),
    }
    respx.post("http://localhost:11434/api/generate").mock(
        return_value=httpx.Response(200, json=response_body)
    )
    result = await _call_ollama(
        model="phi3:3.8b",
        prompt="test",
        system="test",
        temperature=0.2,
        settings=_test_settings(),
    )
    assert result["relevance"] == 8
    assert result["sentiment"] == "bullish"


@respx.mock
@pytest.mark.asyncio
async def test_call_ollama_malformed_json() -> None:
    response_body = {"response": "not valid json {{{"}
    respx.post("http://localhost:11434/api/generate").mock(
        return_value=httpx.Response(200, json=response_body)
    )
    result = await _call_ollama(
        model="phi3:3.8b",
        prompt="test",
        system="test",
        temperature=0.2,
        settings=_test_settings(),
    )
    assert result == {}


@respx.mock
@pytest.mark.asyncio
async def test_call_ollama_http_error() -> None:
    respx.post("http://localhost:11434/api/generate").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    result = await _call_ollama(
        model="phi3:3.8b",
        prompt="test",
        system="test",
        temperature=0.2,
        settings=_test_settings(),
    )
    assert result == {}


@respx.mock
@pytest.mark.asyncio
async def test_filter_article_returns_result() -> None:
    response_body = {
        "response": json.dumps(
            {
                "relevance": 7,
                "sentiment": "bearish",
                "tickers": ["AAPL"],
                "key_facts": ["Revenue missed estimates"],
            }
        ),
    }
    respx.post("http://localhost:11434/api/generate").mock(
        return_value=httpx.Response(200, json=response_body)
    )
    article = ArticleRecord(
        title="Apple Revenue Miss",
        url="https://test.com",
        source="test",
        content="Apple reported lower than expected revenue.",
    )
    result = await filter_article(article, settings=_test_settings())
    assert result.relevance == 7
    assert result.sentiment == "bearish"
    assert "AAPL" in result.tickers


@respx.mock
@pytest.mark.asyncio
async def test_check_ollama_health_ok() -> None:
    respx.get("http://localhost:11434/api/tags").mock(
        return_value=httpx.Response(200, json={"models": [{"name": "phi3:3.8b"}]})
    )
    result = await check_ollama_health(settings=_test_settings())
    assert result is True


@respx.mock
@pytest.mark.asyncio
async def test_check_ollama_health_no_models() -> None:
    respx.get("http://localhost:11434/api/tags").mock(
        return_value=httpx.Response(200, json={"models": []})
    )
    result = await check_ollama_health(settings=_test_settings())
    assert result is False


def test_filter_prompt_formatting() -> None:
    prompt = FILTER_PROMPT_TEMPLATE.format(title="Test Article", content="Some financial content.")
    assert "Test Article" in prompt
    assert "Some financial content." in prompt


def test_analysis_prompt_formatting() -> None:
    prompt = ANALYSIS_PROMPT_TEMPLATE.format(
        market_data="- SPY: $450",
        articles="- Test article",
        history="No history",
        date="2026-03-01",
        sources_count=5,
    )
    assert "SPY: $450" in prompt
    assert "2026-03-01" in prompt


def test_format_market_data() -> None:
    records = [
        MarketDataRecord(
            ticker="SPY",
            name="S&P 500 ETF",
            price=450.0,
            change_1w_pct=1.5,
            change_1m_pct=3.0,
            volume=100000,
        )
    ]
    result = _format_market_data(records)
    assert "SPY" in result
    assert "$450.00" in result


def test_format_articles() -> None:
    records = [
        ArticleRecord(
            title="Test",
            url="https://test.com",
            source="test",
            content="Some content",
            sentiment="bullish",
        )
    ]
    result = _format_articles(records)
    assert "[bullish]" in result
    assert "Test" in result


def test_filter_result_with_missing_fields() -> None:
    result = FilterResult(relevance=0, sentiment="neutral", tickers=[], key_facts=[])
    assert result.relevance == 0
    assert result.sentiment == "neutral"
