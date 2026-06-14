# core/telegram.py
"""
Thin async wrapper around python-telegram-bot.
All agents use this to send alerts and reports.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

from core.config import settings

log = structlog.get_logger(__name__)

_MAX_MESSAGE_LEN = 4096  # Telegram hard limit


class TelegramNotifier:
    """
    Singleton-friendly notifier. Lazily creates the Bot instance.
    Use send_alert() for errors/warnings, send_report() for scheduled reports.
    """

    _bot: Optional[Bot] = None

    def _get_bot(self) -> Bot:
        if self._bot is None:
            self.__class__._bot = Bot(token=settings.telegram_bot_token)
        return self._bot

    async def send_alert(self, message: str, *, parse_mode: str = ParseMode.MARKDOWN) -> bool:
        """Send to the alert chat (owner or dedicated alerts channel)."""
        return await self._send(settings.telegram_alert_chat_id, message, parse_mode=parse_mode)

    async def send_report(self, message: str, *, parse_mode: str = ParseMode.MARKDOWN) -> bool:
        """Send to the owner chat (for daily/weekly reports)."""
        return await self._send(settings.telegram_owner_chat_id, message, parse_mode=parse_mode)

    async def _send(self, chat_id: str, message: str, *, parse_mode: str) -> bool:
        bot = self._get_bot()
        # Split if over Telegram's limit
        chunks = _split_message(message)
        success = True
        for chunk in chunks:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
            except TelegramError as exc:
                log.error("telegram.send_error", chat_id=chat_id, error=str(exc))
                success = False
        return success


def _split_message(text: str) -> list[str]:
    if len(text) <= _MAX_MESSAGE_LEN:
        return [text]
    chunks = []
    while text:
        chunks.append(text[:_MAX_MESSAGE_LEN])
        text = text[_MAX_MESSAGE_LEN:]
    return chunks


# Module-level singleton
_notifier: Optional[TelegramNotifier] = None


def get_notifier() -> TelegramNotifier:
    global _notifier
    if _notifier is None:
        _notifier = TelegramNotifier()
    return _notifier


async def send_alert(message: str) -> bool:
    return await get_notifier().send_alert(message)


async def send_report(message: str) -> bool:
    return await get_notifier().send_report(message)
