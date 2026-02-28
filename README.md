# Investment Automation

Monthly investment recommendation pipeline that scrapes financial news and market data, analyzes them with a local LLM (Ollama), and delivers recommendations via Telegram.

## Prerequisites

- Docker and Docker Compose
- 16 GB RAM recommended (8 GB minimum)
- ~15 GB disk for LLM models

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set your Telegram credentials:

```
TELEGRAM_BOT_TOKEN=<your-bot-token>
TELEGRAM_CHAT_ID=<your-chat-id>
```

To get these:
1. Create a bot via [@BotFather](https://t.me/BotFather) on Telegram and copy the token.
2. Send any message to your bot.
3. Visit `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` and find `"chat":{"id": <CHAT_ID>}`.

### 2. Start services

```bash
docker compose up -d
```

This starts three services:
- **ollama** — LLM inference server (pulls `phi3:mini` and `mistral:7b` on first start, takes a few minutes)
- **airflow** — Scheduler and web UI at [http://localhost:8080](http://localhost:8080) (admin/admin)
- **worker** — Python container that runs the pipeline

Wait for Ollama to finish pulling models. You can check progress with:

```bash
docker compose logs -f ollama
```

### 3. Run the pipeline

```bash
docker compose run --rm worker python -m src.main
```

The pipeline will:
1. Check Ollama health and loaded models
2. Scrape RSS feeds (CNBC, MarketWatch, Investing.com, Dow Jones)
3. Fetch market data (yfinance with Stooq/Google Finance fallbacks)
4. Deduplicate and store articles in SQLite
5. Pre-filter articles with `phi3:mini` (relevance scoring)
6. Deep analysis with `mistral:7b` (recommendation generation)
7. Send the recommendation to Telegram

Expected runtime: 15-45 minutes depending on CPU speed and article count.

### 4. Automated monthly runs

Airflow is preconfigured to run the pipeline on the 1st of every month at 08:00 UTC. Access the Airflow UI at [http://localhost:8080](http://localhost:8080) to monitor, trigger manually, or adjust the schedule.

## Market Data Sources

Market data uses a fallback chain — if one source fails, the next is tried automatically:

| Priority | Source | Method | Notes |
|----------|--------|--------|-------|
| 1 | Yahoo Finance | `yfinance` batch download | Rate-limited frequently |
| 2 | Stooq | CSV download | No API key, reliable |
| 3 | Google Finance | HTML scraping | Price only (no change %) |

The pipeline logs which source was used for each ticker.

### Default tickers

`^IBEX`, `^GSPC`, `ACWI`, `VWCE.DE`, `IUSN.DE`, `AGGH.DE`, `SPY`, `QQQ`

Override in `.env` or `src/config.py`.

## Notification Backends

Set `NOTIFICATION_BACKENDS` in `.env`. Multiple backends can run simultaneously.

| Backend | Env vars required |
|---------|------------------|
| `telegram` | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` |
| `ntfy` | `NTFY_TOPIC` (optional: `NTFY_SERVER`, `NTFY_TOKEN`) |
| `email` | `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `EMAIL_TO` |
| `twilio` | `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `MY_WHATSAPP_NUMBER` |

Example for Telegram only:
```
NOTIFICATION_BACKENDS=["telegram"]
```

## Project Structure

```
├── docker-compose.yml          # Service definitions
├── Dockerfile                  # Worker image
├── Dockerfile.airflow          # Airflow image
├── .env.example                # Environment template
├── scripts/
│   └── ollama-entrypoint.sh    # Pulls LLM models on startup
├── dags/
│   └── monthly_investment.py   # Airflow DAG
├── src/
│   ├── main.py                 # Pipeline entry point
│   ├── config.py               # Settings (Pydantic)
│   ├── scrapers/               # RSS, web, and market data scrapers
│   ├── pipeline/               # Cleaning, dedup, SQLite storage
│   ├── analysis/               # Ollama LLM client and prompts
│   ├── recommendations/        # Recommendation generation and history
│   └── delivery/               # Telegram, WhatsApp, email, ntfy
└── tests/
```

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

Lint and format:
```bash
ruff check src/
ruff format src/
```

Type check:
```bash
mypy src/
```

## Troubleshooting

**Ollama not ready:** The health check waits up to 5 minutes for models to load. Check logs with `docker compose logs ollama`.

**Yahoo Finance rate-limited:** This is expected. The pipeline falls back to Stooq automatically. Logs will show `yfinance_missing_tickers` followed by `stooq_ticker_ok`.

**Telegram not sending:** Test delivery in isolation:
```bash
docker compose run --rm worker python -c "
import asyncio, httpx
TOKEN = '<your-token>'
CHAT_ID = '<your-chat-id>'
async def test():
    r = await httpx.AsyncClient().post(
        f'https://api.telegram.org/bot{TOKEN}/sendMessage',
        json={'chat_id': CHAT_ID, 'text': 'Test'})
    print(r.status_code, r.text)
asyncio.run(test())
"
```

**DNS issues in containers:** The worker service uses Google DNS (8.8.8.8). If you're behind a corporate firewall, update the `dns` section in `docker-compose.yml`.

## Disclaimer

This is a personal decision-support tool, not financial advice. All recommendations should be reviewed manually before acting.
