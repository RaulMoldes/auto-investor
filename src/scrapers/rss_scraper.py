from datetime import UTC, datetime
from time import mktime

import feedparser
import httpx
import structlog

from src.scrapers.base import Article, BaseScraper

logger = structlog.get_logger()


class RssScraper(BaseScraper):
    def __init__(self, feed_urls: list[str], timeout: float = 30.0) -> None:
        self.feed_urls = feed_urls
        self.timeout = timeout

    async def scrape(self) -> list[Article]:
        articles: list[Article] = []
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        async with httpx.AsyncClient(timeout=self.timeout, headers=headers) as client:
            for url in self.feed_urls:
                try:
                    feed_articles = await self._scrape_feed(client, url)
                    articles.extend(feed_articles)
                    logger.info("rss_feed_scraped", url=url, count=len(feed_articles))
                except Exception:
                    logger.exception("rss_feed_error", url=url)
        return articles

    async def _scrape_feed(self, client: httpx.AsyncClient, url: str) -> list[Article]:
        response = await client.get(url, follow_redirects=True)
        response.raise_for_status()
        feed = feedparser.parse(response.text)
        articles: list[Article] = []
        for entry in feed.entries:
            published_at = self._parse_published(entry)
            summary = getattr(entry, "summary", "")
            articles.append(
                Article(
                    title=entry.get("title", ""),
                    url=entry.get("link", ""),
                    source=feed.feed.get("title", url),
                    published_at=published_at,
                    content=summary,
                    raw_text=summary,
                )
            )
        return articles

    @staticmethod
    def _parse_published(entry: object) -> datetime | None:
        parsed = getattr(entry, "published_parsed", None)
        if parsed:
            return datetime.fromtimestamp(mktime(parsed), tz=UTC)
        return None
