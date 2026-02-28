"""Email (SMTP) notification backend.

Setup:
    1. For Gmail: enable 2FA, create an App Password at
       https://myaccount.google.com/apppasswords
    2. Set SMTP_HOST=smtp.gmail.com, SMTP_PORT=587, SMTP_USER, SMTP_PASSWORD, EMAIL_TO.
"""

import json
from email.message import EmailMessage

import aiosmtplib
import structlog

from src.config import Settings
from src.delivery.base import BaseNotifier
from src.pipeline.storage import RecommendationRecord

logger = structlog.get_logger()


class EmailNotifier(BaseNotifier):
    def __init__(self, settings: Settings) -> None:
        self.host = settings.smtp_host
        self.port = settings.smtp_port
        self.user = settings.smtp_user
        self.password = settings.smtp_password
        self.email_to = settings.email_to

    def format_message(self, rec: RecommendationRecord) -> str:
        """Format as HTML for email clients."""
        month_name = rec.date.strftime("%B %Y")
        assets = json.loads(rec.assets_json) if rec.assets_json else []
        key_factors = json.loads(rec.key_factors_json) if rec.key_factors_json else []
        risks = json.loads(rec.risks_json) if rec.risks_json else []

        asset_rows = "".join(
            f"<tr><td><code>{_esc(a.get('ticker', '?'))}</code></td>"
            f"<td>{a.get('allocation_pct', 0)}%</td>"
            f"<td>{_esc(a.get('name', ''))}</td></tr>"
            for a in assets
        )

        risk_label = {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High"}.get(
            rec.risk_level, rec.risk_level
        )

        return f"""\
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: auto;">
<h2>Monthly Recommendation &mdash; {_esc(month_name)}</h2>

<h3>Market Summary</h3>
<p>{_esc(rec.market_summary)}</p>

<h3>Recommendation: {_esc(rec.action)}</h3>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
<tr><th>Ticker</th><th>Allocation</th><th>Name</th></tr>
{asset_rows}
</table>

<p><strong>Risk Level:</strong> {_esc(risk_label)}<br>
<strong>Confidence:</strong> {rec.confidence:.0%}</p>

<h3>Justification</h3>
<p>{_esc(rec.justification)}</p>

<p><strong>Key Factors:</strong> {_esc(", ".join(key_factors))}<br>
<strong>Risks:</strong> {_esc(", ".join(risks))}<br>
<strong>Sources analyzed:</strong> {rec.sources_used}</p>

<hr>
<p style="font-size: 0.85em; color: #666;">
<em>This is an automated analysis for personal use only.
Not financial advice. Always do your own research before investing.</em>
</p>
</body>
</html>"""

    async def send(self, message: str) -> bool:
        if not self.host or not self.user or not self.email_to:
            logger.warning("email_not_configured")
            return False

        msg = EmailMessage()
        msg["Subject"] = "Monthly Investment Recommendation"
        msg["From"] = self.user
        msg["To"] = self.email_to
        msg.set_content("See the HTML version of this email for the full recommendation.")
        msg.add_alternative(message, subtype="html")

        try:
            await aiosmtplib.send(
                msg,
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                start_tls=True,
            )
            logger.info("email_sent", to=self.email_to)
            return True
        except Exception:
            logger.exception("email_send_failed")
            return False


def _esc(text: str) -> str:
    """Escape HTML special characters."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
