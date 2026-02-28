import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "investment-automation",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="monthly_investment_recommendation",
    default_args=DEFAULT_ARGS,
    description="Monthly investment recommendation pipeline",
    schedule="0 8 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["investment", "monthly"],
)
def monthly_investment_dag() -> None:
    @task()
    def check_ollama_health() -> bool:
        from src.analysis.llm_client import check_ollama_health as _check

        result = asyncio.run(_check())
        if not result:
            raise RuntimeError("Ollama health check failed. No models loaded.")
        return result

    @task()
    def scrape_rss() -> int:
        from src.pipeline.aggregator import scrape_rss as _scrape

        articles = asyncio.run(_scrape())
        return len(articles)

    @task()
    def scrape_web() -> int:
        from src.pipeline.aggregator import scrape_web as _scrape

        articles = asyncio.run(_scrape())
        return len(articles)

    @task()
    def fetch_market_data() -> int:
        from src.pipeline.aggregator import fetch_market_data as _fetch

        data = asyncio.run(_fetch())
        return len(data)

    @task(trigger_rule="none_failed_min_one_success")
    def aggregate_data(
        rss_count: int = 0, web_count: int = 0, market_count: int = 0
    ) -> str:
        from src.pipeline.aggregator import (
            aggregate_and_store,
            fetch_market_data as _fetch_market,
            scrape_rss as _scrape_rss,
            scrape_web as _scrape_web,
        )

        pipe_run_id = str(uuid.uuid4())[:8]

        rss_articles = asyncio.run(_scrape_rss())
        web_articles = asyncio.run(_scrape_web())
        market_data = asyncio.run(_fetch_market())

        all_articles = rss_articles + web_articles
        aggregate_and_store(all_articles, market_data, pipe_run_id)
        return pipe_run_id

    @task()
    def prefilter_articles(pipeline_run_id: str) -> str:
        from src.analysis.llm_client import filter_article
        from src.config import get_settings
        from src.pipeline.storage import get_articles_by_run

        settings = get_settings()
        articles = get_articles_by_run(pipeline_run_id)

        for article in articles:
            try:
                result = asyncio.run(filter_article(article, settings))
                article.relevance_score = float(result.relevance)
                article.sentiment = result.sentiment
                article.tickers_mentioned = ",".join(result.tickers)
            except Exception:
                pass

        return pipeline_run_id

    @task()
    def deep_analysis(pipeline_run_id: str) -> str:
        import json

        from src.analysis.llm_client import analyze_dataset
        from src.pipeline.storage import get_articles_by_run, get_market_data_by_run
        from src.recommendations.portfolio import get_history_summary

        articles = get_articles_by_run(pipeline_run_id)
        market_data = get_market_data_by_run(pipeline_run_id)
        history = get_history_summary()

        result = asyncio.run(
            analyze_dataset(articles, market_data, history_summary=history)
        )
        return json.dumps(result, ensure_ascii=False)

    @task()
    def generate_recommendation(
        analysis_json: str = "", pipeline_run_id: str = ""
    ) -> int:
        import json

        from src.recommendations.generator import generate_and_store

        llm_output = json.loads(analysis_json)
        rec = generate_and_store(llm_output, pipeline_run_id)
        return rec.id if rec and rec.id else 0

    @task()
    def send_notification(pipeline_run_id: str = "", rec_id: int = 0) -> bool:
        from src.delivery.base import dispatch_notification
        from src.pipeline.storage import get_recommendation_by_id

        rec = get_recommendation_by_id(rec_id)
        if not rec:
            return False
        return asyncio.run(dispatch_notification(rec))

    @task()
    def log_run_result(
        notification_sent: bool = False, pipeline_run_id: str = ""
    ) -> None:
        from datetime import datetime, timezone

        from src.pipeline.storage import update_run_log

        update_run_log(
            pipeline_run_id,
            status="success",
            finished_at=datetime.now(tz=timezone.utc),
            whatsapp_sent=notification_sent,
        )

    # Task graph
    health = check_ollama_health()
    rss = scrape_rss()
    web = scrape_web()
    market = fetch_market_data()

    health >> rss >> web >> market

    agg_run_id = aggregate_data(rss, web, market)
    filtered_run_id = prefilter_articles(agg_run_id)
    analysis_json = deep_analysis(filtered_run_id)
    rec_id = generate_recommendation(analysis_json, filtered_run_id)
    sent = send_notification(filtered_run_id, rec_id)
    log_run_result(sent, filtered_run_id)


monthly_investment_dag()
