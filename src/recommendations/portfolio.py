import json

import structlog

from src.config import get_settings
from src.pipeline.storage import RecommendationRecord, get_recent_recommendations

logger = structlog.get_logger()


def get_history_summary(months: int | None = None) -> str:
    settings = get_settings()
    limit = months or settings.recommendation_history_months
    records = get_recent_recommendations(limit=limit)

    if not records:
        return "No previous recommendations available."

    return _format_records(records)


def _format_records(records: list[RecommendationRecord]) -> str:
    lines: list[str] = []
    for rec in records:
        assets = json.loads(rec.assets_json) if rec.assets_json else []
        asset_str = ", ".join(
            f"{a.get('ticker', '?')} ({a.get('allocation_pct', 0)}%)" for a in assets
        )
        lines.append(
            f"- {rec.date.strftime('%Y-%m-%d')}: {rec.action} "
            f"(confidence: {rec.confidence:.0%}, risk: {rec.risk_level}). "
            f"Assets: {asset_str or 'N/A'}"
        )
    return "\n".join(lines)
