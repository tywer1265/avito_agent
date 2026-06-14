# core/base_agent.py
"""
BaseAgent — every agent inherits from this class.
Provides: Claude API calls, cost tracking, retry logic,
          DB logging, Telegram alerts, schema validation.
"""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Type, TypeVar

import anthropic
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import settings
from core.database import AgentLog, get_session

log = structlog.get_logger(__name__)

T = TypeVar("T")

# Approximate cost per 1M tokens (USD) — update if Anthropic changes pricing
_SONNET_COST_PER_1M_INPUT = 3.00
_SONNET_COST_PER_1M_OUTPUT = 15.00
_HAIKU_COST_PER_1M_INPUT = 0.25
_HAIKU_COST_PER_1M_OUTPUT = 1.25


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    if "haiku" in model.lower():
        return (
            input_tokens * _HAIKU_COST_PER_1M_INPUT / 1_000_000
            + output_tokens * _HAIKU_COST_PER_1M_OUTPUT / 1_000_000
        )
    return (
        input_tokens * _SONNET_COST_PER_1M_INPUT / 1_000_000
        + output_tokens * _SONNET_COST_PER_1M_OUTPUT / 1_000_000
    )


class AgentError(Exception):
    """Raised when an agent cannot recover from a failure."""


class BaseAgent(ABC):
    """
    Abstract base for all Avito agents.

    Subclasses must implement:
        execute(task: dict) -> dict
    """

    name: str = "base_agent"

    def __init__(self) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        self._monthly_cost: float = 0.0  # session accumulator; real tracking is in DB
        self._log = structlog.get_logger(self.name)
        # Import here to avoid circular imports; telegram module is thin
        from core.telegram import TelegramNotifier
        self._telegram = TelegramNotifier()

    # ── Public interface ──────────────────────────────────────

    @abstractmethod
    async def execute(self, task: dict) -> dict:
        """Run the agent's primary task. Must return a dict."""

    async def run(self, task: dict) -> dict:
        """
        Wraps execute() with top-level error handling.
        Always returns a dict — never raises to the scheduler.
        """
        start = time.monotonic()
        try:
            result = await self.execute(task)
            elapsed = time.monotonic() - start
            self._log.info("agent.run.ok", elapsed=round(elapsed, 2))
            return result
        except Exception as exc:
            elapsed = time.monotonic() - start
            self._log.error("agent.run.failed", error=str(exc), elapsed=round(elapsed, 2))
            return await self.handle_failure(exc)

    # ── Claude API helpers ────────────────────────────────────

    async def call_sonnet(
        self,
        system: str,
        user: str,
        *,
        temperature: float = 0.3,
        max_tokens: Optional[int] = None,
    ) -> str:
        return await self._call_claude(
            model=settings.claude_sonnet_model,
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens or settings.anthropic_max_tokens_sonnet,
        )

    async def call_haiku(
        self,
        system: str,
        user: str,
        *,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
    ) -> str:
        return await self._call_claude(
            model=settings.claude_haiku_model,
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens or settings.anthropic_max_tokens_haiku,
        )

    async def _call_claude(
        self,
        model: str,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
    ) -> str:
        await self._check_budget()

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type((anthropic.APIConnectionError, anthropic.RateLimitError)),
            reraise=True,
        ):
            with attempt:
                response = await self._client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system,
                    messages=[{"role": "user", "content": user}],
                )

        cost = _estimate_cost(model, response.usage.input_tokens, response.usage.output_tokens)
        self._monthly_cost += cost
        text = response.content[0].text

        await self.log(
            action=f"claude_call:{model.split('-')[1]}",
            result=text[:200],
            confidence_score=1.0,
            cost=cost,
        )
        return text

    # ── JSON helpers ──────────────────────────────────────────

    async def call_sonnet_json(self, system: str, user: str, **kwargs) -> dict:
        """Call Sonnet and parse the response as JSON. Retries on parse failure."""
        raw = await self.call_sonnet(system, user + "\n\nRespond ONLY with valid JSON.", **kwargs)
        return self._parse_json(raw)

    async def call_haiku_json(self, system: str, user: str, **kwargs) -> dict:
        raw = await self.call_haiku(system, user + "\n\nRespond ONLY with valid JSON.", **kwargs)
        return self._parse_json(raw)

    @staticmethod
    def _parse_json(raw: str) -> dict:
        # Strip markdown fences if present
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
            cleaned = cleaned.rsplit("```", 1)[0]
        return json.loads(cleaned.strip())

    # ── Logging ───────────────────────────────────────────────

    async def log(
        self,
        action: str,
        result: Any,
        confidence_score: float = 1.0,
        cost: float = 0.0,
        status: str = "ok",
        input_summary: str = "",
    ) -> None:
        self._log.info(
            "agent.action",
            action=action,
            status=status,
            confidence=confidence_score,
            cost=round(cost, 6),
        )
        try:
            async with get_session() as session:
                entry = AgentLog(
                    agent_name=self.name,
                    action=action,
                    input=input_summary[:1000] if input_summary else None,
                    output=str(result)[:1000],
                    cost=Decimal(str(round(cost, 6))),
                    confidence=confidence_score,
                    status=status,
                )
                session.add(entry)
        except Exception as db_err:
            self._log.error("agent.log.db_error", error=str(db_err))

    # ── Error handling ────────────────────────────────────────

    async def handle_failure(self, error: Exception) -> dict:
        msg = f"[{self.name}] Failure: {type(error).__name__}: {error}"
        self._log.error("agent.failure", error=msg)
        await self.log(action="failure", result=msg, confidence_score=0.0, status="error")
        await self.report_to_telegram(f"🚨 *{self.name}* error:\n`{str(error)[:300]}`")
        return {"status": "error", "agent": self.name, "error": str(error)}

    # ── Telegram ──────────────────────────────────────────────

    async def report_to_telegram(self, message: str) -> None:
        try:
            await self._telegram.send_alert(message)
        except Exception as exc:
            self._log.error("agent.telegram_error", error=str(exc))

    # ── Budget guard ──────────────────────────────────────────

    async def _check_budget(self) -> None:
        """
        Query monthly Claude spend from DB. Alert at $250, raise at $280.
        """
        try:
            from sqlalchemy import func, select
            async with get_session() as session:
                result = await session.execute(
                    select(func.sum(AgentLog.cost)).where(
                        AgentLog.action.like("claude_call:%"),
                        AgentLog.timestamp >= _month_start(),
                    )
                )
                monthly_spend: float = float(result.scalar() or 0)

            alert_threshold = settings.anthropic_monthly_budget_usd - 30
            if monthly_spend >= settings.anthropic_monthly_budget_usd:
                raise AgentError(
                    f"Monthly Claude budget exhausted: ${monthly_spend:.2f} "
                    f">= ${settings.anthropic_monthly_budget_usd:.2f}"
                )
            if monthly_spend >= alert_threshold:
                await self.report_to_telegram(
                    f"⚠️ Claude spend alert: ${monthly_spend:.2f} / "
                    f"${settings.anthropic_monthly_budget_usd:.2f}"
                )
        except AgentError:
            raise
        except Exception as exc:
            # Don't block the call if budget check itself fails
            self._log.warning("agent.budget_check_failed", error=str(exc))

    # ── Schema validation ─────────────────────────────────────

    @staticmethod
    def validate_schema(data: dict, required_keys: list[str], context: str = "") -> None:
        missing = [k for k in required_keys if k not in data]
        if missing:
            raise ValueError(f"Schema validation failed [{context}]: missing keys {missing}")


def _month_start() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
