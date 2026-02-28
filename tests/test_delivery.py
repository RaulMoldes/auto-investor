import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from src.config import Settings
from src.delivery.base import BaseNotifier, dispatch_notification
from src.delivery.email_notifier import EmailNotifier, _esc
from src.delivery.ntfy import NtfyNotifier
from src.delivery.telegram import TelegramNotifier, _escape_md
from src.delivery.whatsapp import (
    DISCLAIMER,
    TwilioNotifier,
    format_whatsapp_message,
    send_whatsapp,
)
from src.pipeline.storage import RecommendationRecord


def _sample_recommendation() -> RecommendationRecord:
    return RecommendationRecord(
        run_id="test1",
        date=datetime(2026, 3, 1, tzinfo=UTC),
        action="BUY",
        risk_level="MEDIUM",
        confidence=0.75,
        market_summary="Markets showed resilience with broad-based gains.",
        justification="Strong earnings and dovish Fed tone support equity allocation.",
        assets_json=json.dumps(
            [
                {"ticker": "VWCE.DE", "name": "Vanguard All-World", "allocation_pct": 60},
                {"ticker": "IUSN.DE", "name": "MSCI World Small Cap", "allocation_pct": 20},
                {"ticker": "AGGH.DE", "name": "Global Aggregate Bond", "allocation_pct": 20},
            ]
        ),
        key_factors_json=json.dumps(["Strong earnings", "ECB rate pause"]),
        risks_json=json.dumps(["Geopolitical tensions", "Inflation"]),
        sources_used=42,
    )


def _test_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {"sqlite_db_path": ":memory:"}
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


# ── BaseNotifier interface contract ──


def test_base_notifier_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError):
        BaseNotifier()  # type: ignore[abstract]


# ── Twilio / WhatsApp (existing tests preserved) ──


def test_format_whatsapp_message() -> None:
    rec = _sample_recommendation()
    message = format_whatsapp_message(rec)

    assert "March 2026" in message
    assert "BUY" in message
    assert "VWCE.DE" in message
    assert "60%" in message
    assert "75%" in message
    assert "Medium" in message
    assert "Strong earnings" in message
    assert "42" in message
    assert DISCLAIMER in message


def test_format_whatsapp_message_empty_assets() -> None:
    rec = _sample_recommendation()
    rec.assets_json = "[]"
    message = format_whatsapp_message(rec)
    assert "Recommendation: BUY" in message


def test_send_whatsapp_not_configured() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(twilio_account_sid="", twilio_auth_token="")
    result = send_whatsapp(rec, settings=settings)
    assert result is False


def test_send_whatsapp_no_number() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(
        twilio_account_sid="ACTEST", twilio_auth_token="token", my_whatsapp_number=""
    )
    result = send_whatsapp(rec, settings=settings)
    assert result is False


@patch("src.delivery.whatsapp.TwilioClient")
def test_send_whatsapp_success(mock_twilio_cls: MagicMock) -> None:
    mock_client = MagicMock()
    mock_message = MagicMock()
    mock_message.sid = "SM1234567890"
    mock_client.messages.create.return_value = mock_message
    mock_twilio_cls.return_value = mock_client

    rec = _sample_recommendation()
    settings = _test_settings(
        twilio_account_sid="ACTEST",
        twilio_auth_token="token",
        twilio_whatsapp_from="whatsapp:+1234",
        my_whatsapp_number="whatsapp:+5678",
    )
    result = send_whatsapp(rec, settings=settings)
    assert result is True
    mock_client.messages.create.assert_called_once()
    call_kwargs = mock_client.messages.create.call_args.kwargs
    assert call_kwargs["from_"] == "whatsapp:+1234"
    assert call_kwargs["to"] == "whatsapp:+5678"


@patch("src.delivery.whatsapp.TwilioClient")
def test_send_whatsapp_api_error(mock_twilio_cls: MagicMock) -> None:
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = Exception("Twilio error")
    mock_twilio_cls.return_value = mock_client

    rec = _sample_recommendation()
    settings = _test_settings(
        twilio_account_sid="ACTEST",
        twilio_auth_token="token",
        twilio_whatsapp_from="whatsapp:+1234",
        my_whatsapp_number="whatsapp:+5678",
    )
    result = send_whatsapp(rec, settings=settings)
    assert result is False


@pytest.mark.asyncio
async def test_twilio_notifier_send_not_configured() -> None:
    settings = _test_settings(twilio_account_sid="", twilio_auth_token="")
    notifier = TwilioNotifier(settings)
    result = await notifier.send("test message")
    assert result is False


# ── Telegram ──


def test_telegram_format_message_contains_key_info() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(telegram_bot_token="tok", telegram_chat_id="123")
    notifier = TelegramNotifier(settings)
    message = notifier.format_message(rec)

    assert "March 2026" in message
    assert "VWCE.DE" in message
    assert "60%" in message
    assert "75%" in message


def test_telegram_escape_md() -> None:
    assert _escape_md("Hello *world*") == "Hello \\*world\\*"
    assert _escape_md("a_b.c") == "a\\_b\\.c"


@respx.mock
@pytest.mark.asyncio
async def test_telegram_send_success() -> None:
    respx.post("https://api.telegram.org/bottok123/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    settings = _test_settings(telegram_bot_token="tok123", telegram_chat_id="999")
    notifier = TelegramNotifier(settings)
    result = await notifier.send("Hello")
    assert result is True


@respx.mock
@pytest.mark.asyncio
async def test_telegram_send_api_error() -> None:
    respx.post("https://api.telegram.org/bottok123/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": False, "description": "Bad Request"})
    )
    settings = _test_settings(telegram_bot_token="tok123", telegram_chat_id="999")
    notifier = TelegramNotifier(settings)
    result = await notifier.send("Hello")
    assert result is False


@pytest.mark.asyncio
async def test_telegram_send_not_configured() -> None:
    settings = _test_settings(telegram_bot_token="", telegram_chat_id="")
    notifier = TelegramNotifier(settings)
    result = await notifier.send("Hello")
    assert result is False


# ── ntfy ──


def test_ntfy_format_message_plain_text() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(ntfy_topic="test-topic")
    notifier = NtfyNotifier(settings)
    message = notifier.format_message(rec)

    assert "VWCE.DE" in message
    assert "BUY" in message
    assert "*" not in message  # no markdown
    assert "<" not in message  # no HTML


@respx.mock
@pytest.mark.asyncio
async def test_ntfy_send_success() -> None:
    respx.post("https://ntfy.sh/my-topic").mock(return_value=httpx.Response(200, text="ok"))
    settings = _test_settings(ntfy_topic="my-topic")
    notifier = NtfyNotifier(settings)
    result = await notifier.send("Test notification")
    assert result is True


@respx.mock
@pytest.mark.asyncio
async def test_ntfy_send_with_auth() -> None:
    route = respx.post("https://self-hosted.example.com/my-topic").mock(
        return_value=httpx.Response(200, text="ok")
    )
    settings = _test_settings(
        ntfy_topic="my-topic",
        ntfy_server="https://self-hosted.example.com",
        ntfy_token="secret-token",
    )
    notifier = NtfyNotifier(settings)
    result = await notifier.send("Test notification")
    assert result is True
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret-token"


@pytest.mark.asyncio
async def test_ntfy_send_not_configured() -> None:
    settings = _test_settings(ntfy_topic="")
    notifier = NtfyNotifier(settings)
    result = await notifier.send("Hello")
    assert result is False


# ── Email ──


def test_email_format_message_html() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(smtp_host="smtp.test.com", smtp_user="u", email_to="e")
    notifier = EmailNotifier(settings)
    message = notifier.format_message(rec)

    assert "<html>" in message
    assert "VWCE.DE" in message
    assert "March 2026" in message
    assert "<table" in message
    assert "60%" in message


def test_email_esc() -> None:
    assert _esc("<script>alert('xss')</script>") == "&lt;script&gt;alert('xss')&lt;/script&gt;"
    assert _esc("A & B") == "A &amp; B"


@pytest.mark.asyncio
async def test_email_send_not_configured() -> None:
    settings = _test_settings(smtp_host="", smtp_user="", email_to="")
    notifier = EmailNotifier(settings)
    result = await notifier.send("<html>test</html>")
    assert result is False


@pytest.mark.asyncio
async def test_email_send_success() -> None:
    settings = _test_settings(
        smtp_host="smtp.test.com",
        smtp_port=587,
        smtp_user="user@test.com",
        smtp_password="pass",
        email_to="dest@test.com",
    )
    notifier = EmailNotifier(settings)
    with patch("src.delivery.email_notifier.aiosmtplib") as mock_smtp:
        mock_smtp.send = AsyncMock(return_value=({}, "OK"))
        result = await notifier.send("<html>Hello</html>")
    assert result is True


@pytest.mark.asyncio
async def test_email_send_failure() -> None:
    settings = _test_settings(
        smtp_host="smtp.test.com",
        smtp_user="user@test.com",
        smtp_password="pass",
        email_to="dest@test.com",
    )
    notifier = EmailNotifier(settings)
    with patch("src.delivery.email_notifier.aiosmtplib") as mock_smtp:
        mock_smtp.send = AsyncMock(side_effect=Exception("SMTP error"))
        result = await notifier.send("<html>Hello</html>")
    assert result is False


# ── Multi-backend dispatch ──


@respx.mock
@pytest.mark.asyncio
async def test_dispatch_single_backend_telegram() -> None:
    respx.post("https://api.telegram.org/bottok/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    rec = _sample_recommendation()
    settings = _test_settings(
        notification_backends=["telegram"],
        telegram_bot_token="tok",
        telegram_chat_id="123",
    )
    result = await dispatch_notification(rec, settings)
    assert result is True


@pytest.mark.asyncio
async def test_dispatch_no_backends() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(notification_backends=[])
    result = await dispatch_notification(rec, settings)
    assert result is False


@respx.mock
@pytest.mark.asyncio
async def test_dispatch_multi_backend_one_fails_one_succeeds() -> None:
    """If one backend fails but another succeeds, dispatch returns True."""
    respx.post("https://api.telegram.org/bottok/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    rec = _sample_recommendation()
    settings = _test_settings(
        notification_backends=["ntfy", "telegram"],
        ntfy_topic="",  # not configured → will return False
        telegram_bot_token="tok",
        telegram_chat_id="123",
    )
    result = await dispatch_notification(rec, settings)
    assert result is True


@respx.mock
@pytest.mark.asyncio
async def test_dispatch_multi_backend_all_fail() -> None:
    """If all backends fail, dispatch returns False."""
    rec = _sample_recommendation()
    settings = _test_settings(
        notification_backends=["ntfy", "telegram"],
        ntfy_topic="",
        telegram_bot_token="",
        telegram_chat_id="",
    )
    result = await dispatch_notification(rec, settings)
    assert result is False


@pytest.mark.asyncio
async def test_dispatch_unknown_backend_ignored() -> None:
    rec = _sample_recommendation()
    settings = _test_settings(notification_backends=["nonexistent"])
    result = await dispatch_notification(rec, settings)
    assert result is False
