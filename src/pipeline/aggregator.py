import structlog

from src.config import Settings, get_settings
from src.pipeline.cleaner import clean_article, deduplicate_articles
from src.pipeline.storage import (
    ArticleRecord,
    MarketDataRecord,
    store_articles,
    store_market_data,
)
from src.scrapers.base import Article
from src.scrapers.rss_scraper import RssScraper
from src.scrapers.web_scraper import WebScraper
from src.scrapers.yahoo_finance import MarketDataPoint, YahooFinanceScraper

logger = structlog.get_logger()


def article_to_record(article: Article) -> ArticleRecord:
    return ArticleRecord(
        title=article.title,
        url=article.url,
        source=article.source,
        published_at=article.published_at,
        content=article.content,
        raw_text=article.raw_text,
    )


def market_data_to_record(data: MarketDataPoint) -> MarketDataRecord:
    return MarketDataRecord(
        ticker=data.ticker,
        name=data.name,
        price=data.price,
        change_1w_pct=data.change_1w_pct,
        change_1m_pct=data.change_1m_pct,
        volume=data.volume,
        fetched_at=data.fetched_at,
    )


async def scrape_rss(settings: Settings | None = None) -> list[Article]:
    settings = settings or get_settings()
    scraper = RssScraper(feed_urls=settings.rss_feed_urls)
    return await scraper.scrape()


async def scrape_web(settings: Settings | None = None) -> list[Article]:
    settings = settings or get_settings()
    scraper = WebScraper(targets=settings.web_scraper_targets)
    return await scraper.scrape()


async def fetch_market_data(settings: Settings | None = None) -> list[MarketDataPoint]:
    settings = settings or get_settings()
    scraper = YahooFinanceScraper(tickers=settings.yahoo_tickers)
    return await scraper.scrape()


def aggregate_and_store(
    articles: list[Article],
    market_data: list[MarketDataPoint],
    run_id: str,
    settings: Settings | None = None,
) -> tuple[list[int], int]:
    settings = settings or get_settings()

    cleaned = [clean_article(a, max_tokens=settings.max_article_tokens) for a in articles]
    deduped = deduplicate_articles(cleaned, settings.dedup_similarity_threshold)

    records = [article_to_record(a) for a in deduped]
    article_ids = store_articles(records, run_id=run_id)

    market_records = [market_data_to_record(d) for d in market_data]
    store_market_data(market_records, run_id=run_id)

    logger.info(
        "aggregation_complete",
        run_id=run_id,
        articles_stored=len(article_ids),
        market_records=len(market_records),
    )
    return article_ids, len(market_records)
