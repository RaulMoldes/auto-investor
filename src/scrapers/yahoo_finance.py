import asyncio
import csv
import io
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from src.scrapers.base import BaseScraper

logger = structlog.get_logger()

MAX_RETRIES = 3
BASE_BACKOFF = 5.0
INTER_TICKER_DELAY = (2.0, 3.0)

STOOQ_CSV_URL = "https://stooq.com/q/d/l/"
GOOGLE_FINANCE_URL = "https://www.google.com/finance/quote/{ticker}"

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Mapping for Stooq ticker format (Yahoo -> Stooq)
STOOQ_TICKER_MAP: dict[str, str] = {
    "^GSPC": "^SPX",
    "^IBEX": "^IBEX",
    "SPY": "SPY.US",
    "QQQ": "QQQ.US",
    "ACWI": "ACWI.US",
    "VWCE.DE": "VWCE.DE",
    "IUSN.DE": "IUSN.DE",
    "AGGH.DE": "AGGH.DE",
}

# Mapping for Google Finance ticker format (Yahoo -> Google)
GOOGLE_TICKER_MAP: dict[str, str] = {
    "^GSPC": ".INX:INDEXSP",
    "^IBEX": "IBEX:BME",
    "SPY": "SPY:NYSEARCA",
    "QQQ": "QQQ:NASDAQ",
    "ACWI": "ACWI:NASDAQ",
    "VWCE.DE": "VWCE:ETR",
    "IUSN.DE": "IUSN:ETR",
    "AGGH.DE": "AGGH:ETR",
}


@dataclass
class MarketDataPoint:
    ticker: str
    name: str
    price: float
    change_1w_pct: float
    change_1m_pct: float
    volume: int
    fetched_at: datetime
    source: str = "yfinance"


class YahooFinanceScraper(BaseScraper):
    def __init__(self, tickers: list[str]) -> None:
        self.tickers = tickers

    async def scrape(self) -> list["MarketDataPoint"]:  # type: ignore[override]
        results = await self._batch_yfinance()

        # Find tickers that failed and try fallbacks
        fetched_tickers = {r.ticker for r in results}
        missing = [t for t in self.tickers if t not in fetched_tickers]

        if missing:
            logger.info("yfinance_missing_tickers", missing=missing, trying="stooq")
            fallback_results = await self._fallback_stooq(missing)
            results.extend(fallback_results)

            still_missing = [
                t for t in missing if t not in {r.ticker for r in fallback_results}
            ]
            if still_missing:
                logger.info(
                    "stooq_missing_tickers", missing=still_missing, trying="google_finance"
                )
                google_results = await self._fallback_google_finance(still_missing)
                results.extend(google_results)

        fetched = {r.ticker for r in results}
        final_missing = [t for t in self.tickers if t not in fetched]
        if final_missing:
            logger.warning("market_data_unavailable", tickers=final_missing)

        logger.info(
            "market_scrape_complete",
            total=len(self.tickers),
            success=len(results),
            failed=len(final_missing) if final_missing else 0,
            sources={r.source for r in results},
        )
        return results

    # ── Primary: yfinance batch download ─────────────────────────────

    async def _batch_yfinance(self) -> list[MarketDataPoint]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return await asyncio.to_thread(self._yf_download_sync)
            except Exception:
                if attempt < MAX_RETRIES:
                    backoff = BASE_BACKOFF * (2 ** (attempt - 1))
                    logger.warning(
                        "yfinance_batch_retry",
                        attempt=attempt,
                        backoff=backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.exception("yfinance_batch_failed")
        return []

    def _yf_download_sync(self) -> list[MarketDataPoint]:
        import yfinance as yf  # noqa: PLC0415

        tickers_str = " ".join(self.tickers)
        logger.info("yfinance_batch_download", tickers=tickers_str)

        df = yf.download(
            tickers_str,
            period="1mo",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=False,
        )

        if df.empty:
            logger.warning("yfinance_empty_dataframe")
            return []

        results: list[MarketDataPoint] = []
        single_ticker = len(self.tickers) == 1

        for ticker in self.tickers:
            try:
                if single_ticker:
                    ticker_df = df
                else:
                    if ticker not in df.columns.get_level_values(0):
                        logger.warning("yfinance_ticker_missing_in_df", ticker=ticker)
                        continue
                    ticker_df = df[ticker]

                closes = ticker_df["Close"].dropna()
                if closes.empty:
                    logger.warning("yfinance_no_closes", ticker=ticker)
                    continue

                volumes = ticker_df["Volume"].dropna()
                current_price = float(closes.iloc[-1])
                volume = int(volumes.iloc[-1]) if not volumes.empty else 0
                price_1w = float(closes.iloc[-5]) if len(closes) >= 5 else current_price
                price_1m = float(closes.iloc[0])

                change_1w = ((current_price - price_1w) / price_1w) * 100 if price_1w else 0.0
                change_1m = ((current_price - price_1m) / price_1m) * 100 if price_1m else 0.0

                info = yf.Ticker(ticker).fast_info
                name = getattr(info, "short_name", ticker) or ticker

                results.append(
                    MarketDataPoint(
                        ticker=ticker,
                        name=name if isinstance(name, str) else ticker,
                        price=round(current_price, 2),
                        change_1w_pct=round(change_1w, 2),
                        change_1m_pct=round(change_1m, 2),
                        volume=volume,
                        fetched_at=datetime.now(tz=UTC),
                        source="yfinance",
                    )
                )
                logger.info("yfinance_ticker_ok", ticker=ticker)
            except Exception:
                logger.exception("yfinance_ticker_parse_error", ticker=ticker)

        return results

    # ── Fallback 1: Stooq CSV downloads ──────────────────────────────

    async def _fallback_stooq(self, tickers: list[str]) -> list[MarketDataPoint]:
        results: list[MarketDataPoint] = []
        today = datetime.now(tz=UTC).date()
        month_ago = today - timedelta(days=30)

        async with httpx.AsyncClient(
            timeout=30.0, headers=COMMON_HEADERS, follow_redirects=True
        ) as client:
            for i, ticker in enumerate(tickers):
                stooq_ticker = STOOQ_TICKER_MAP.get(ticker, ticker)
                try:
                    params = {
                        "s": stooq_ticker,
                        "d1": month_ago.strftime("%Y%m%d"),
                        "d2": today.strftime("%Y%m%d"),
                        "i": "d",
                    }
                    resp = await client.get(STOOQ_CSV_URL, params=params)
                    resp.raise_for_status()

                    point = self._parse_stooq_csv(ticker, resp.text)
                    if point:
                        results.append(point)
                        logger.info("stooq_ticker_ok", ticker=ticker)
                    else:
                        logger.warning("stooq_no_data", ticker=ticker)
                except Exception:
                    logger.exception("stooq_ticker_failed", ticker=ticker)

                if i < len(tickers) - 1:
                    await asyncio.sleep(random.uniform(*INTER_TICKER_DELAY))  # noqa: S311

        return results

    @staticmethod
    def _parse_stooq_csv(ticker: str, csv_text: str) -> MarketDataPoint | None:
        reader = csv.DictReader(io.StringIO(csv_text))
        rows = list(reader)
        if not rows or "Close" not in rows[0]:
            return None

        closes = [float(r["Close"]) for r in rows if r.get("Close")]
        volumes = [int(float(r["Volume"])) for r in rows if r.get("Volume")]

        if not closes:
            return None

        current_price = closes[-1]
        volume = volumes[-1] if volumes else 0
        price_1w = closes[-5] if len(closes) >= 5 else current_price
        price_1m = closes[0]

        change_1w = ((current_price - price_1w) / price_1w) * 100 if price_1w else 0.0
        change_1m = ((current_price - price_1m) / price_1m) * 100 if price_1m else 0.0

        return MarketDataPoint(
            ticker=ticker,
            name=ticker,
            price=round(current_price, 2),
            change_1w_pct=round(change_1w, 2),
            change_1m_pct=round(change_1m, 2),
            volume=volume,
            fetched_at=datetime.now(tz=UTC),
            source="stooq",
        )

    # ── Fallback 2: Google Finance scraping ──────────────────────────

    async def _fallback_google_finance(self, tickers: list[str]) -> list[MarketDataPoint]:
        results: list[MarketDataPoint] = []
        async with httpx.AsyncClient(
            timeout=30.0, headers=COMMON_HEADERS, follow_redirects=True
        ) as client:
            for i, ticker in enumerate(tickers):
                google_ticker = GOOGLE_TICKER_MAP.get(ticker, ticker)
                try:
                    url = GOOGLE_FINANCE_URL.format(ticker=google_ticker)
                    resp = await client.get(url)
                    resp.raise_for_status()

                    point = self._parse_google_finance(ticker, resp.text)
                    if point:
                        results.append(point)
                        logger.info("google_finance_ticker_ok", ticker=ticker)
                    else:
                        logger.warning("google_finance_no_data", ticker=ticker)
                except Exception:
                    logger.exception("google_finance_ticker_failed", ticker=ticker)

                if i < len(tickers) - 1:
                    await asyncio.sleep(random.uniform(*INTER_TICKER_DELAY))  # noqa: S311

        return results

    @staticmethod
    def _parse_google_finance(ticker: str, html: str) -> MarketDataPoint | None:
        from bs4 import BeautifulSoup  # noqa: PLC0415

        soup = BeautifulSoup(html, "lxml")

        # Google Finance puts the price in a div with data-last-price attribute
        price_el = soup.find(attrs={"data-last-price": True})
        if not price_el:
            return None

        try:
            price = float(price_el["data-last-price"])  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return None

        # Google Finance doesn't provide 1w/1m change easily from a single page,
        # so we set them to 0.0 (better than no data at all)
        return MarketDataPoint(
            ticker=ticker,
            name=ticker,
            price=round(price, 2),
            change_1w_pct=0.0,
            change_1m_pct=0.0,
            volume=0,
            fetched_at=datetime.now(tz=UTC),
            source="google_finance",
        )
