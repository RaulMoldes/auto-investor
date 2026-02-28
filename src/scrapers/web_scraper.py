import asyncio
import random
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
import structlog
from bs4 import BeautifulSoup

from src.scrapers.base import Article, BaseScraper

logger = structlog.get_logger()


@dataclass
class WebTarget:
    url: str
    title_selector: str
    summary_selector: str


class WebScraper(BaseScraper):
    def __init__(self, targets: list[dict[str, str]], timeout: float = 30.0) -> None:
        self.targets = [
            WebTarget(
                url=t["url"],
                title_selector=t["title_selector"],
                summary_selector=t["summary_selector"],
            )
            for t in targets
        ]
        self.timeout = timeout

    async def scrape(self) -> list[Article]:
        articles: list[Article] = []
        for i, target in enumerate(self.targets):
            try:
                headers = self._build_headers(target.url)
                async with httpx.AsyncClient(
                    timeout=self.timeout,
                    headers=headers,
                    follow_redirects=True,
                ) as client:
                    target_articles = await self._scrape_target(client, target)
                    articles.extend(target_articles)
                    logger.info("web_target_scraped", url=target.url, count=len(target_articles))
            except Exception:
                logger.exception("web_target_error", url=target.url)
            if i < len(self.targets) - 1:
                await asyncio.sleep(random.uniform(1.0, 3.0))  # noqa: S311
        return articles

    @staticmethod
    def _build_headers(url: str) -> dict[str, str]:
        parsed = urlparse(url)
        return {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"{parsed.scheme}://{parsed.netloc}/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    async def _scrape_target(self, client: httpx.AsyncClient, target: WebTarget) -> list[Article]:
        response = await client.get(target.url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        parsed_base = urlparse(target.url)
        base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

        titles = soup.select(target.title_selector)
        summaries = soup.select(target.summary_selector)

        articles: list[Article] = []
        for i, title_el in enumerate(titles):
            title_text = title_el.get_text(strip=True)
            link = title_el.get("href", "")
            if isinstance(link, list):
                link = link[0] if link else ""
            if link and not link.startswith("http"):
                link = f"{base_url}{link}"

            summary_text = ""
            if i < len(summaries):
                summary_text = summaries[i].get_text(strip=True)

            articles.append(
                Article(
                    title=title_text,
                    url=str(link),
                    source=target.url,
                    content=summary_text,
                    raw_text=summary_text,
                )
            )
        return articles
