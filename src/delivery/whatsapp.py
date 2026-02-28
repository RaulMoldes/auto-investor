import json

import structlog
from twilio.rest import Client as TwilioClient

from src.config import Settings, get_settings
from src.delivery.base import BaseNotifier
from src.pipeline.storage import RecommendationRecord

logger = structlog.get_logger()

DISCLAIMER = (
    "\n\n_This is an automated analysis for personal use only. "
    "Not financial advice. Always do your own research before investing._"
)


class TwilioNotifier(BaseNotifier):
    def __init__(self, settings: Settings) -> None:
        self.account_sid = settings.twilio_account_sid
        self.auth_token = settings.twilio_auth_token
        self.from_number = settings.twilio_whatsapp_from
        self.to_number = settings.my_whatsapp_number

    def format_message(self, rec: RecommendationRecord) -> str:
        return format_whatsapp_message(rec)

    async def send(self, message: str) -> bool:
        if not self.account_sid or not self.auth_token:
            logger.warning("twilio_not_configured")
            return False
        if not self.to_number:
            logger.warning("whatsapp_number_not_set")
            return False

        try:
            client = TwilioClient(self.account_sid, self.auth_token)
            result = client.messages.create(
                body=message,
                from_=self.from_number,
                to=self.to_number,
            )
            logger.info("whatsapp_sent", sid=result.sid, to=self.to_number)
            return True
        except Exception:
            logger.exception("whatsapp_send_failed")
            return False


def format_whatsapp_message(rec: RecommendationRecord) -> str:
    month_name = rec.date.strftime("%B %Y")
    assets = json.loads(rec.assets_json) if rec.assets_json else []
    key_factors = json.loads(rec.key_factors_json) if rec.key_factors_json else []
    risks = json.loads(rec.risks_json) if rec.risks_json else []

    asset_lines = "\n".join(
        f"  * {a.get('ticker', '?')} ({a.get('allocation_pct', 0)}%) - {a.get('name', '')}"
        for a in assets
    )

    risk_emoji = {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High"}.get(
        rec.risk_level, rec.risk_level
    )

    message = (
        f"Monthly Recommendation - {month_name}\n\n"
        f"Market Summary:\n{rec.market_summary}\n\n"
        f"Recommendation: {rec.action}\n"
        f"{asset_lines}\n\n"
        f"Risk Level: {risk_emoji}\n"
        f"Confidence: {rec.confidence:.0%}\n\n"
        f"Justification:\n{rec.justification}\n\n"
        f"Key Factors: {', '.join(key_factors)}\n"
        f"Risks: {', '.join(risks)}\n"
        f"Sources analyzed: {rec.sources_used}"
        f"{DISCLAIMER}"
    )
    return message


def send_whatsapp(rec: RecommendationRecord, settings: Settings | None = None) -> bool:
    """Legacy sync wrapper kept for backward compatibility."""
    settings = settings or get_settings()

    if not settings.twilio_account_sid or not settings.twilio_auth_token:
        logger.warning("twilio_not_configured")
        return False

    if not settings.my_whatsapp_number:
        logger.warning("whatsapp_number_not_set")
        return False

    message_body = format_whatsapp_message(rec)

    try:
        client = TwilioClient(settings.twilio_account_sid, settings.twilio_auth_token)
        message = client.messages.create(
            body=message_body,
            from_=settings.twilio_whatsapp_from,
            to=settings.my_whatsapp_number,
        )
        logger.info("whatsapp_sent", sid=message.sid, to=settings.my_whatsapp_number)
        return True
    except Exception:
        logger.exception("whatsapp_send_failed")
        return False
