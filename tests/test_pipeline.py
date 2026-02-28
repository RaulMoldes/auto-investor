import sqlite3

from src.pipeline.cleaner import (
    clean_article,
    deduplicate_articles,
    normalize_whitespace,
    strip_html,
    truncate_to_tokens,
)
from src.pipeline.storage import (
    ArticleRecord,
    MarketDataRecord,
    RecommendationRecord,
    RunLogRecord,
    _CREATE_TABLES_SQL,
)
from src.scrapers.base import Article


def test_strip_html() -> None:
    assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert strip_html("") == ""
    assert strip_html("plain text") == "plain text"


def test_normalize_whitespace() -> None:
    assert normalize_whitespace("  hello   world  ") == "hello world"
    assert normalize_whitespace("a\n\nb\tc") == "a b c"


def test_truncate_to_tokens() -> None:
    text = "one two three four five"
    assert truncate_to_tokens(text, 3) == "one two three"
    assert truncate_to_tokens(text, 10) == text


def test_clean_article() -> None:
    article = Article(
        title="<b>Test Title</b>",
        url="https://example.com",
        source="test",
        content="<p>Some   HTML    content</p>",
    )
    cleaned = clean_article(article, max_tokens=3)
    assert cleaned.title == "Test Title"
    assert cleaned.content == "Some HTML content"


def test_deduplicate_by_url() -> None:
    articles = [
        Article(title="Markets rally on Fed decision", url="https://a.com", source="s"),
        Article(title="Different title entirely about oil prices", url="https://a.com", source="s"),
        Article(
            title="Completely unrelated topic about technology",
            url="https://b.com",
            source="s",
        ),
    ]
    result = deduplicate_articles(articles)
    assert len(result) == 2


def test_deduplicate_by_title_similarity() -> None:
    articles = [
        Article(title="Markets rally on Fed decision", url="https://a.com", source="s"),
        Article(title="Markets rally on Fed decision today", url="https://b.com", source="s"),
        Article(title="Oil prices drop sharply", url="https://c.com", source="s"),
    ]
    result = deduplicate_articles(articles, similarity_threshold=0.8)
    assert len(result) == 2


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_CREATE_TABLES_SQL)
    return conn


def test_storage_crud_in_memory() -> None:
    conn = _make_db()
    conn.execute(
        """INSERT INTO articles (title, url, source, content, run_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("Test", "https://test.com", "test", "content", "run1", "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM articles").fetchone()
    assert row is not None
    assert row["title"] == "Test"
    assert row["run_id"] == "run1"


def test_storage_market_data_in_memory() -> None:
    conn = _make_db()
    conn.execute(
        """INSERT INTO market_data (ticker, name, price, change_1w_pct, change_1m_pct, volume, fetched_at, run_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("SPY", "S&P 500 ETF", 450.0, 1.5, 3.0, 100000, "2026-01-01T00:00:00+00:00", "run1"),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM market_data").fetchone()
    assert row is not None
    assert row["ticker"] == "SPY"
    assert row["price"] == 450.0


def test_storage_recommendation_in_memory() -> None:
    conn = _make_db()
    conn.execute(
        """INSERT INTO recommendations
           (run_id, date, action, risk_level, confidence, market_summary, justification, assets_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "run1",
            "2026-01-01T00:00:00+00:00",
            "BUY",
            "MEDIUM",
            0.75,
            "Markets up",
            "Strong earnings",
            '[{"ticker": "SPY"}]',
        ),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM recommendations").fetchone()
    assert row is not None
    assert row["action"] == "BUY"
    assert row["confidence"] == 0.75


def test_run_log_in_memory() -> None:
    conn = _make_db()
    conn.execute(
        """INSERT INTO run_logs (run_id, started_at, status, articles_scraped,
           articles_filtered, recommendation_generated, whatsapp_sent)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("run1", "2026-01-01T00:00:00+00:00", "running", 10, 0, 0, 0),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM run_logs").fetchone()
    assert row is not None
    assert row["run_id"] == "run1"
    assert row["articles_scraped"] == 10
