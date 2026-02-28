"""ntfy.sh notification backend.

Setup:
    1. Pick a topic name (e.g., "investment-recs-xyz123"). Keep it unique to avoid spam.
    2. Subscribe on your phone: install the ntfy app, add topic.
    3. Set NTFY_TOPIC in your .env file.
    4. For self-hosted ntfy, also set NTFY_SERVER and optionally NTFY_TOKEN.
"""

import json

import httpx
import structlog

from src.config import Settings
from src.delivery.base import BaseNotifier
from src.pipeline.storage import RecommendationRecord

logger = structlog.get_logger()


class NtfyNotifier(BaseNotifier):
    def __init__(self, settings: Settings) -> None:
        self.topic = settings.ntfy_topic
        self.server = settings.ntfy_server
        self.token = settings.ntfy_token

    def format_message(self, rec: RecommendationRecord) -> str:
        """Format as plain text for ntfy push notifications."""
        assets = json.loads(rec.assets_json) if rec.assets_json else []
        key_factors = json.loads(rec.key_factors_json) if rec.key_factors_json else []
        risks = json.loads(rec.risks_json) if rec.risks_json else []

        asset_lines = "\n".join(
            f"  - {a.get('ticker', '?')} ({a.get('allocation_pct', 0)}%) - {a.get('name', '')}"
            for a in assets
        )

        risk_label = {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High"}.get(
            rec.risk_level, rec.risk_level
        )

        parts = [
            f"Market Summary:\n{rec.market_summary}",
            "",
            f"Recommendation: {rec.action}",
            asset_lines,
            "",
            f"Risk Level: {risk_label}",
            f"Confidence: {rec.confidence:.0%}",
            "",
            f"Justification:\n{rec.justification}",
            "",
            f"Key Factors: {', '.join(key_factors)}",
            f"Risks: {', '.join(risks)}",
            f"Sources analyzed: {rec.sources_used}",
            "",
            "This is automated analysis, not financial advice.",
        ]
        return "\n".join(parts)

    async def send(self, message: str) -> bool:
        if not self.topic:
            logger.warning("ntfy_not_configured")
            return False

        url = f"{self.server}/{self.topic}"
        headers: dict[str, str] = {
            "Title": "Monthly Investment Recommendation",
            "Priority": "high",
            "Tags": "chart_with_upwards_trend,moneybag",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, content=message, headers=headers)
                response.raise_for_status()
                logger.info("ntfy_sent", topic=self.topic)
                return True
        except Exception:
            logger.exception("ntfy_send_failed")
            return False
