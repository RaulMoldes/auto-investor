import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from src.config import get_settings

logger = structlog.get_logger()

_CREATE_TABLES_SQL = """\
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    published_at TEXT,
    content TEXT DEFAULT '',
    raw_text TEXT DEFAULT '',
    relevance_score REAL,
    sentiment TEXT,
    tickers_mentioned TEXT,
    created_at TEXT NOT NULL,
    run_id TEXT
);

CREATE TABLE IF NOT EXISTS market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    name TEXT DEFAULT '',
    price REAL DEFAULT 0.0,
    change_1w_pct REAL DEFAULT 0.0,
    change_1m_pct REAL DEFAULT 0.0,
    volume INTEGER DEFAULT 0,
    fetched_at TEXT NOT NULL,
    run_id TEXT
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    date TEXT NOT NULL,
    action TEXT DEFAULT '',
    risk_level TEXT DEFAULT '',
    confidence REAL DEFAULT 0.0,
    market_summary TEXT DEFAULT '',
    justification TEXT DEFAULT '',
    assets_json TEXT DEFAULT '',
    key_factors_json TEXT DEFAULT '',
    risks_json TEXT DEFAULT '',
    sources_used INTEGER DEFAULT 0,
    raw_llm_output TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS run_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT DEFAULT 'running',
    articles_scraped INTEGER DEFAULT 0,
    articles_filtered INTEGER DEFAULT 0,
    recommendation_generated INTEGER DEFAULT 0,
    whatsapp_sent INTEGER DEFAULT 0,
    error_message TEXT
);
"""


def _dt_to_str(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _str_to_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s)


@dataclass
class ArticleRecord:
    title: str
    url: str
    source: str
    published_at: datetime | None = None
    content: str = ""
    raw_text: str = ""
    relevance_score: float | None = None
    sentiment: str | None = None
    tickers_mentioned: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str | None = None
    id: int | None = None


@dataclass
class MarketDataRecord:
    ticker: str
    name: str = ""
    price: float = 0.0
    change_1w_pct: float = 0.0
    change_1m_pct: float = 0.0
    volume: int = 0
    fetched_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str | None = None
    id: int | None = None


@dataclass
class RecommendationRecord:
    run_id: str
    date: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    action: str = ""
    risk_level: str = ""
    confidence: float = 0.0
    market_summary: str = ""
    justification: str = ""
    assets_json: str = ""
    key_factors_json: str = ""
    risks_json: str = ""
    sources_used: int = 0
    raw_llm_output: str = ""
    id: int | None = None


@dataclass
class RunLogRecord:
    run_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    status: str = "running"
    articles_scraped: int = 0
    articles_filtered: int = 0
    recommendation_generated: bool = False
    whatsapp_sent: bool = False
    error_message: str | None = None
    id: int | None = None


_connection: sqlite3.Connection | None = None


def get_connection() -> sqlite3.Connection:
    global _connection  # noqa: PLW0603
    if _connection is None:
        settings = get_settings()
        _connection = sqlite3.connect(settings.sqlite_db_path)
        _connection.row_factory = sqlite3.Row
        _connection.executescript(_CREATE_TABLES_SQL)
        logger.info("database_initialized", path=settings.sqlite_db_path)
    return _connection


def reset_connection() -> None:
    """Close and reset the cached connection (useful for tests)."""
    global _connection  # noqa: PLW0603
    if _connection is not None:
        _connection.close()
        _connection = None


def store_articles(articles: list[ArticleRecord], run_id: str) -> list[int]:
    stored_ids: list[int] = []
    duplicates = 0
    conn = get_connection()
    for article in articles:
        article.run_id = run_id
        try:
            cursor = conn.execute(
                """INSERT INTO articles
                   (title, url, source, published_at, content, raw_text,
                    relevance_score, sentiment, tickers_mentioned, created_at, run_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    article.title,
                    article.url,
                    article.source,
                    _dt_to_str(article.published_at),
                    article.content,
                    article.raw_text,
                    article.relevance_score,
                    article.sentiment,
                    article.tickers_mentioned,
                    _dt_to_str(article.created_at),
                    article.run_id,
                ),
            )
            stored_ids.append(cursor.lastrowid or 0)
        except sqlite3.IntegrityError:
            duplicates += 1
    conn.commit()
    logger.info(
        "articles_stored",
        count=len(stored_ids),
        duplicates=duplicates,
        run_id=run_id,
    )
    return stored_ids


def store_market_data(records: list[MarketDataRecord], run_id: str) -> None:
    conn = get_connection()
    for record in records:
        record.run_id = run_id
        conn.execute(
            """INSERT INTO market_data
               (ticker, name, price, change_1w_pct, change_1m_pct, volume, fetched_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.ticker,
                record.name,
                record.price,
                record.change_1w_pct,
                record.change_1m_pct,
                record.volume,
                _dt_to_str(record.fetched_at),
                record.run_id,
            ),
        )
    conn.commit()
    logger.info("market_data_stored", count=len(records), run_id=run_id)


def store_recommendation(record: RecommendationRecord) -> int:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO recommendations
           (run_id, date, action, risk_level, confidence, market_summary,
            justification, assets_json, key_factors_json, risks_json,
            sources_used, raw_llm_output)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            record.run_id,
            _dt_to_str(record.date),
            record.action,
            record.risk_level,
            record.confidence,
            record.market_summary,
            record.justification,
            record.assets_json,
            record.key_factors_json,
            record.risks_json,
            record.sources_used,
            record.raw_llm_output,
        ),
    )
    conn.commit()
    record.id = cursor.lastrowid
    rec_id = record.id if record.id is not None else 0
    logger.info("recommendation_stored", id=rec_id, run_id=record.run_id)
    return rec_id


def _row_to_article(row: sqlite3.Row) -> ArticleRecord:
    return ArticleRecord(
        id=row["id"],
        title=row["title"],
        url=row["url"],
        source=row["source"],
        published_at=_str_to_dt(row["published_at"]),
        content=row["content"] or "",
        raw_text=row["raw_text"] or "",
        relevance_score=row["relevance_score"],
        sentiment=row["sentiment"],
        tickers_mentioned=row["tickers_mentioned"],
        created_at=_str_to_dt(row["created_at"]) or datetime.now(tz=UTC),
        run_id=row["run_id"],
    )


def _row_to_market_data(row: sqlite3.Row) -> MarketDataRecord:
    return MarketDataRecord(
        id=row["id"],
        ticker=row["ticker"],
        name=row["name"] or "",
        price=row["price"] or 0.0,
        change_1w_pct=row["change_1w_pct"] or 0.0,
        change_1m_pct=row["change_1m_pct"] or 0.0,
        volume=row["volume"] or 0,
        fetched_at=_str_to_dt(row["fetched_at"]) or datetime.now(tz=UTC),
        run_id=row["run_id"],
    )


def _row_to_recommendation(row: sqlite3.Row) -> RecommendationRecord:
    return RecommendationRecord(
        id=row["id"],
        run_id=row["run_id"],
        date=_str_to_dt(row["date"]) or datetime.now(tz=UTC),
        action=row["action"] or "",
        risk_level=row["risk_level"] or "",
        confidence=row["confidence"] or 0.0,
        market_summary=row["market_summary"] or "",
        justification=row["justification"] or "",
        assets_json=row["assets_json"] or "",
        key_factors_json=row["key_factors_json"] or "",
        risks_json=row["risks_json"] or "",
        sources_used=row["sources_used"] or 0,
        raw_llm_output=row["raw_llm_output"] or "",
    )


def get_articles_by_run(run_id: str) -> list[ArticleRecord]:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM articles WHERE run_id = ?", (run_id,)).fetchall()
    return [_row_to_article(r) for r in rows]


def get_market_data_by_run(run_id: str) -> list[MarketDataRecord]:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM market_data WHERE run_id = ?", (run_id,)).fetchall()
    return [_row_to_market_data(r) for r in rows]


def get_recent_recommendations(limit: int = 3) -> list[RecommendationRecord]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM recommendations ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()
    return [_row_to_recommendation(r) for r in rows]


def get_recommendation_by_id(rec_id: int) -> RecommendationRecord | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM recommendations WHERE id = ?", (rec_id,)).fetchone()
    if row is None:
        return None
    return _row_to_recommendation(row)


def create_run_log(run_id: str) -> RunLogRecord:
    record = RunLogRecord(run_id=run_id)
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO run_logs (run_id, started_at, status, articles_scraped,
           articles_filtered, recommendation_generated, whatsapp_sent)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            record.run_id,
            _dt_to_str(record.started_at),
            record.status,
            record.articles_scraped,
            record.articles_filtered,
            int(record.recommendation_generated),
            int(record.whatsapp_sent),
        ),
    )
    conn.commit()
    record.id = cursor.lastrowid
    return record


def update_run_log(run_id: str, **kwargs: Any) -> None:
    conn = get_connection()
    if not kwargs:
        return
    set_clauses: list[str] = []
    values: list[Any] = []
    for key, value in kwargs.items():
        set_clauses.append(f"{key} = ?")
        if isinstance(value, datetime):
            values.append(_dt_to_str(value))
        elif isinstance(value, bool):
            values.append(int(value))
        else:
            values.append(value)
    values.append(run_id)
    conn.execute(
        f"UPDATE run_logs SET {', '.join(set_clauses)} WHERE run_id = ?",  # noqa: S608
        values,
    )
    conn.commit()
