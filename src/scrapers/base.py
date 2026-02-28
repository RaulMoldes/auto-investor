from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Article:
    title: str
    url: str
    source: str
    published_at: datetime | None = None
    content: str = ""
    raw_text: str = ""
    metadata: dict[str, str] = field(default_factory=dict)


class BaseScraper(ABC):
    @abstractmethod
    async def scrape(self) -> list[Article]: ...
