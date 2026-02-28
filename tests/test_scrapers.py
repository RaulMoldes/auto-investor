import json

import httpx
import pytest
import respx

from src.scrapers.rss_scraper import RssScraper
from src.scrapers.web_scraper import WebScraper
from src.scrapers.yahoo_finance import YahooFinanceScraper

SAMPLE_RSS = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Markets Rally on Fed Decision</title>
      <link>https://example.com/article1</link>
      <description>Stocks rose sharply after the Fed held rates steady.</description>
      <pubDate>Mon, 01 Mar 2026 12:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Oil Prices Surge</title>
      <link>https://example.com/article2</link>
      <description>Crude oil prices jumped 3% on supply concerns.</description>
      <pubDate>Tue, 02 Mar 2026 08:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

SAMPLE_HTML = """\
<html>
<body>
  <a class="title" href="/news/article1">Tech Stocks Soar</a>
  <p class="description">Technology sector leads gains.</p>
  <a class="title" href="/news/article2">Bond Yields Fall</a>
  <p class="description">Treasury yields drop to monthly lows.</p>
</body>
</html>
"""

SAMPLE_YAHOO_RESPONSE = {
    "chart": {
        "result": [
            {
                "meta": {"shortName": "Test ETF", "symbol": "TEST"},
                "indicators": {
                    "quote": [
                        {
                            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                            "volume": [1000, 1100, 1200, 1300, 1400, 1500],
                        }
                    ]
                },
            }
        ]
    }
}


@respx.mock
@pytest.mark.asyncio
async def test_rss_scraper_parses_feed() -> None:
    respx.get("https://test.com/feed.rss").mock(return_value=httpx.Response(200, text=SAMPLE_RSS))
    scraper = RssScraper(feed_urls=["https://test.com/feed.rss"])
    articles = await scraper.scrape()

    assert len(articles) == 2
    assert articles[0].title == "Markets Rally on Fed Decision"
    assert articles[0].url == "https://example.com/article1"
    assert articles[0].source == "Test Feed"
    assert articles[1].title == "Oil Prices Surge"


@respx.mock
@pytest.mark.asyncio
async def test_rss_scraper_handles_error() -> None:
    respx.get("https://bad.com/feed.rss").mock(
        return_value=httpx.Response(500, text="Server Error")
    )
    scraper = RssScraper(feed_urls=["https://bad.com/feed.rss"])
    articles = await scraper.scrape()
    assert articles == []


@respx.mock
@pytest.mark.asyncio
async def test_web_scraper_extracts_articles() -> None:
    respx.get("https://test.com/news").mock(return_value=httpx.Response(200, text=SAMPLE_HTML))
    targets = [
        {
            "url": "https://test.com/news",
            "title_selector": "a.title",
            "summary_selector": "p.description",
        }
    ]
    scraper = WebScraper(targets=targets)
    articles = await scraper.scrape()

    assert len(articles) == 2
    assert articles[0].title == "Tech Stocks Soar"
    assert articles[1].content == "Treasury yields drop to monthly lows."


@respx.mock
@pytest.mark.asyncio
async def test_yahoo_finance_fetch_ticker() -> None:
    respx.get("https://query2.finance.yahoo.com/v8/finance/chart/TEST").mock(
        return_value=httpx.Response(200, json=SAMPLE_YAHOO_RESPONSE)
    )
    scraper = YahooFinanceScraper(tickers=["TEST"])
    results = await scraper.scrape()

    assert len(results) == 1
    result = results[0]
    assert result.ticker == "TEST"
    assert result.name == "Test ETF"
    assert result.price == 105.0
    assert result.volume == 1500
    assert result.change_1w_pct == pytest.approx(4.0, abs=0.1)


@respx.mock
@pytest.mark.asyncio
async def test_yahoo_finance_empty_response() -> None:
    respx.get("https://query2.finance.yahoo.com/v8/finance/chart/BAD").mock(
        return_value=httpx.Response(200, json={"chart": {"result": None}})
    )
    scraper = YahooFinanceScraper(tickers=["BAD"])
    results = await scraper.scrape()
    assert len(results) == 0
