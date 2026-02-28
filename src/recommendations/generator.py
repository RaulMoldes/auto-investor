import json
from typing import Any

import structlog

from src.pipeline.storage import RecommendationRecord, store_recommendation

logger = structlog.get_logger()

REQUIRED_FIELDS = {"date", "market_summary", "recommendation", "justification"}
RECOMMENDATION_FIELDS = {"action", "assets", "risk_level", "confidence"}


def validate_recommendation(data: dict[str, Any]) -> bool:
    if not REQUIRED_FIELDS.issubset(data.keys()):
        logger.warning("recommendation_missing_fields", missing=REQUIRED_FIELDS - data.keys())
        return False
    rec = data.get("recommendation", {})
    if not isinstance(rec, dict):
        return False
    if not RECOMMENDATION_FIELDS.issubset(rec.keys()):
        logger.warning(
            "recommendation_inner_missing_fields",
            missing=RECOMMENDATION_FIELDS - rec.keys(),
        )
        return False
    return True


def build_recommendation_record(data: dict[str, Any], run_id: str) -> RecommendationRecord:
    rec = data.get("recommendation", {})
    if not isinstance(rec, dict):
        rec = {}

    return RecommendationRecord(
        run_id=run_id,
        action=str(rec.get("action", "HOLD")),
        risk_level=str(rec.get("risk_level", "MEDIUM")),
        confidence=float(rec.get("confidence", 0.5) or 0.5),
        market_summary=str(data.get("market_summary", "")),
        justification=str(data.get("justification", "")),
        assets_json=json.dumps(rec.get("assets", []), ensure_ascii=False),
        key_factors_json=json.dumps(data.get("key_factors", []), ensure_ascii=False),
        risks_json=json.dumps(data.get("risks", []), ensure_ascii=False),
        sources_used=int(data.get("sources_used", 0) or 0),
        raw_llm_output=json.dumps(data, ensure_ascii=False),
    )


def generate_and_store(llm_output: dict[str, Any], run_id: str) -> RecommendationRecord | None:
    if not validate_recommendation(llm_output):
        logger.error("recommendation_validation_failed", run_id=run_id)
        fallback = _build_fallback(run_id)
        store_recommendation(fallback)
        return fallback

    record = build_recommendation_record(llm_output, run_id)
    store_recommendation(record)
    logger.info("recommendation_generated", run_id=run_id, action=record.action)
    return record


def _build_fallback(run_id: str) -> RecommendationRecord:
    return RecommendationRecord(
        run_id=run_id,
        action="HOLD",
        risk_level="MEDIUM",
        confidence=0.3,
        market_summary="Unable to generate full analysis. Defaulting to HOLD.",
        justification="LLM output did not pass validation. Manual review recommended.",
        assets_json="[]",
        key_factors_json="[]",
        risks_json='["LLM analysis failed"]',
        sources_used=0,
        raw_llm_output="{}",
    )
