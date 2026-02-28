import asyncio
import uuid
from datetime import UTC, datetime

import structlog

from src.analysis.llm_client import (
    analyze_dataset,
    check_ollama_health,
    filter_article,
)
from src.config import get_settings
from src.delivery.base import dispatch_notification
from src.pipeline.aggregator import (
    aggregate_and_store,
    fetch_market_data,
    scrape_rss,
    scrape_web,
)
from src.pipeline.storage import (
    create_run_log,
    get_articles_by_run,
    get_market_data_by_run,
    update_run_log,
)
from src.recommendations.generator import generate_and_store
from src.recommendations.portfolio import get_history_summary

logger = structlog.get_logger()


async def run_pipeline() -> None:
    settings = get_settings()
    run_id = str(uuid.uuid4())[:8]
    logger.info("pipeline_start", run_id=run_id)
    create_run_log(run_id)

    try:
        # Step 1: Health check
        healthy = await check_ollama_health(settings)
        if not healthy:
            raise RuntimeError("Ollama is not healthy or no models loaded.")

        # Step 2: Scrape data
        rss_articles = await scrape_rss(settings)
        web_articles = await scrape_web(settings)
        market_data = await fetch_market_data(settings)
        all_articles = rss_articles + web_articles

        logger.info(
            "scraping_complete",
            rss=len(rss_articles),
            web=len(web_articles),
            market=len(market_data),
        )

        # Step 3: Aggregate and store
        article_ids, market_count = aggregate_and_store(all_articles, market_data, run_id, settings)
        update_run_log(run_id, articles_scraped=len(article_ids))

        # Step 4: Pre-filter with small model
        stored_articles = get_articles_by_run(run_id)
        filtered: list[object] = []
        for article in stored_articles:
            try:
                result = await filter_article(article, settings)
                article.relevance_score = float(result.relevance)
                article.sentiment = result.sentiment
                article.tickers_mentioned = ",".join(result.tickers)
                if result.relevance >= settings.prefilter_relevance_threshold:
                    filtered.append(article)
            except Exception:
                logger.exception("filter_error", article_id=article.id)

        logger.info("filtering_complete", total=len(stored_articles), passed=len(filtered))
        update_run_log(run_id, articles_filtered=len(filtered))

        # Step 5: Deep analysis
        market_records = get_market_data_by_run(run_id)
        history = get_history_summary()
        llm_output = await analyze_dataset(
            articles=stored_articles,
            market_data=market_records,
            history_summary=history,
            settings=settings,
        )

        # Step 6: Generate recommendation
        rec = generate_and_store(llm_output, run_id)
        if rec:
            update_run_log(run_id, recommendation_generated=True)

            # Step 7: Send notification via configured backends
            sent = await dispatch_notification(rec, settings)
            update_run_log(run_id, whatsapp_sent=sent)

        update_run_log(
            run_id,
            status="success",
            finished_at=datetime.now(tz=UTC),
        )
        logger.info("pipeline_complete", run_id=run_id)

    except Exception as exc:
        logger.exception("pipeline_failed", run_id=run_id)
        update_run_log(
            run_id,
            status="failed",
            finished_at=datetime.now(tz=UTC),
            error_message=str(exc),
        )
        raise


def main() -> None:
    structlog.configure(
        processors=[
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
    )
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
