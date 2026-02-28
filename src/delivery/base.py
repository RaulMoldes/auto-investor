from abc import ABC, abstractmethod

import structlog

from src.config import Settings, get_settings
from src.pipeline.storage import RecommendationRecord

logger = structlog.get_logger()


class BaseNotifier(ABC):
    @abstractmethod
    async def send(self, message: str) -> bool: ...

    @abstractmethod
    def format_message(self, rec: RecommendationRecord) -> str: ...


async def dispatch_notification(
    rec: RecommendationRecord, settings: Settings | None = None
) -> bool:
    """Send notification via all configured backends.

    Returns True if at least one backend succeeds.
    """
    settings = settings or get_settings()
    notifiers = _build_notifiers(settings)

    if not notifiers:
        logger.warning("no_notification_backends_configured")
        return False

    any_success = False
    for notifier in notifiers:
        backend_name = type(notifier).__name__
        try:
            message = notifier.format_message(rec)
            sent = await notifier.send(message)
            if sent:
                any_success = True
                logger.info("notification_sent", backend=backend_name)
            else:
                logger.warning("notification_failed", backend=backend_name)
        except Exception:
            logger.exception("notification_error", backend=backend_name)

    return any_success


def _build_notifiers(settings: Settings) -> list[BaseNotifier]:
    from src.delivery.email_notifier import EmailNotifier
    from src.delivery.ntfy import NtfyNotifier
    from src.delivery.telegram import TelegramNotifier
    from src.delivery.whatsapp import TwilioNotifier

    registry: dict[str, type[BaseNotifier]] = {
        "telegram": TelegramNotifier,
        "ntfy": NtfyNotifier,
        "email": EmailNotifier,
        "twilio": TwilioNotifier,
    }

    backends = settings.notification_backends
    notifiers: list[BaseNotifier] = []
    for name in backends:
        cls = registry.get(name)
        if cls is None:
            logger.warning("unknown_notification_backend", backend=name)
            continue
        notifiers.append(cls(settings))  # type: ignore[call-arg]
    return notifiers
