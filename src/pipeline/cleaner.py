import re
from difflib import SequenceMatcher

import structlog
from bs4 import BeautifulSoup

from src.scrapers.base import Article

logger = structlog.get_logger()


def strip_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "lxml")
    return soup.get_text(separator=" ", strip=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    words = text.split()
    if len(words) <= max_tokens:
        return text
    return " ".join(words[:max_tokens])


def clean_article(article: Article, max_tokens: int = 2000) -> Article:
    cleaned_content = strip_html(article.content)
    cleaned_content = normalize_whitespace(cleaned_content)
    cleaned_content = truncate_to_tokens(cleaned_content, max_tokens)

    cleaned_title = normalize_whitespace(strip_html(article.title))

    article.content = cleaned_content
    article.title = cleaned_title
    article.raw_text = article.raw_text or article.content
    return article


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def deduplicate_articles(
    articles: list[Article], similarity_threshold: float = 0.8
) -> list[Article]:
    seen_urls: set[str] = set()
    unique: list[Article] = []

    for article in articles:
        if article.url in seen_urls:
            continue

        is_duplicate = False
        for existing in unique:
            if title_similarity(article.title, existing.title) >= similarity_threshold:
                is_duplicate = True
                break

        if not is_duplicate:
            seen_urls.add(article.url)
            unique.append(article)

    removed = len(articles) - len(unique)
    logger.info(
        "articles_deduplicated",
        total_input=len(articles),
        new=len(unique),
        removed=removed,
    )
    return unique
