"""Telegram Bot notification backend.

Setup:
    1. Create a bot via @BotFather on Telegram and copy the token.
    2. Start a conversation with your bot (send it any message).
    3. Get your chat_id by visiting:
       https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
       Look for "chat":{"id": <CHAT_ID>} in the response.
    4. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in your .env file.
"""

import json

import httpx
import structlog

from src.config import Settings
from src.delivery.base import BaseNotifier
from src.pipeline.storage import RecommendationRecord

logger = structlog.get_logger()

TELEGRAM_API = "https://api.telegram.org"


class TelegramNotifier(BaseNotifier):
    def __init__(self, settings: Settings) -> None:
        self.token = settings.telegram_bot_token
        self.chat_id = settings.telegram_chat_id

    def format_message(self, rec: RecommendationRecord) -> str:
        """Format as plain text (reliable, no escaping issues)."""
        month_name = rec.date.strftime("%B %Y")
        assets = json.loads(rec.assets_json) if rec.assets_json else []
        key_factors = json.loads(rec.key_factors_json) if rec.key_factors_json else []
        risks = json.loads(rec.risks_json) if rec.risks_json else []

        asset_lines = "\n".join(
            f"  - {a.get('ticker', '?')} ({a.get('allocation_pct', 0)}%) "
            f"- {a.get('name', '')}"
            for a in assets
        )

        risk_label = {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High"}.get(
            rec.risk_level, rec.risk_level
        )

        confidence_pct = f"{rec.confidence:.0%}"

        parts = [
            f"Monthly Recommendation - {month_name}",
            "",
            f"Market Summary:\n{rec.market_summary}",
            "",
            f"Recommendation: {rec.action}",
            asset_lines,
            "",
            f"Risk Level: {risk_label}",
            f"Confidence: {confidence_pct}",
            "",
            f"Justification:\n{rec.justification}",
            "",
            f"Key Factors: {', '.join(key_factors)}",
            f"Risks: {', '.join(risks)}",
            f"Sources analyzed: {rec.sources_used}",
            "",
            "This is an automated analysis for personal use only. "
            "Not financial advice. Always do your own research before investing.",
        ]
        return "\n".join(parts)

    async def send(self, message: str) -> bool:
        if not self.token or not self.chat_id:
            logger.warning("telegram_not_configured")
            return False

        url = f"{TELEGRAM_API}/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("ok"):
                    logger.info("telegram_sent", chat_id=self.chat_id)
                    return True
                logger.warning("telegram_api_error", response=data)
                return False
        except Exception:
            logger.exception("telegram_send_failed")
            return False
