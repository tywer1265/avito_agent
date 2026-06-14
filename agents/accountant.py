# agents/accountant.py
"""
Agent 8 — Accountant
Mission: Track every ruble in and out of the business.
Reports: daily P&L, weekly summary, monthly full report.
All reports sent to Telegram.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import func, select

from core.base_agent import BaseAgent
from core.config import settings
from core.database import AgentLog, Financial, Order, get_session

log = structlog.get_logger("accountant")


class AccountantAgent(BaseAgent):
    name = "accountant"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        if trigger == "daily_pnl":
            return await self._daily_pnl()
        elif trigger == "weekly_summary":
            return await self._weekly_summary()
        elif trigger == "monthly_report":
            return await self._monthly_report()
        elif trigger == "record_transaction":
            return await self._record_transaction(task)
        elif trigger == "expense_check":
            return await self._expense_ratio_check()
        else:
            return {"status": "ok"}

    # ── Daily P&L ──────────────────────────────────────────────

    async def _daily_pnl(self) -> dict:
        today = date.today()
        pnl = await self._calculate_pnl(today, today)
        claude_cost = await self._get_claude_spend_today()

        # Check expense ratio
        await self._expense_ratio_check()

        report = await self._format_daily_pnl(pnl, claude_cost, today)
        await self.report_to_telegram(report)

        await self.log(
            action="daily_pnl",
            result=f"revenue={pnl['total_revenue']} profit={pnl['net_profit']} margin={pnl['margin_pct']}%",
            confidence_score=1.0,
        )
        return {"status": "ok", "pnl": pnl}

    async def _calculate_pnl(self, date_from: date, date_to: date) -> dict:
        start = datetime.combine(date_from, datetime.min.time()).replace(tzinfo=timezone.utc)
        end = datetime.combine(date_to, datetime.max.time()).replace(tzinfo=timezone.utc)

        async with get_session() as session:
            # Revenue: completed orders
            revenue = await session.scalar(
                select(func.sum(Order.price)).where(
                    Order.created_at >= start,
                    Order.created_at <= end,
                    Order.status.in_(["done", "confirmed"]),
                )
            ) or Decimal("0")

            # Income from financials
            fin_income = await session.scalar(
                select(func.sum(Financial.amount)).where(
                    Financial.date >= start,
                    Financial.date <= end,
                    Financial.type == "income",
                )
            ) or Decimal("0")

            # Expenses
            expenses = await session.scalar(
                select(func.sum(Financial.amount)).where(
                    Financial.date >= start,
                    Financial.date <= end,
                    Financial.type == "expense",
                )
            ) or Decimal("0")

            # Commissions
            commissions = await session.scalar(
                select(func.sum(Financial.amount)).where(
                    Financial.date >= start,
                    Financial.date <= end,
                    Financial.type == "commission",
                )
            ) or Decimal("0")

            # Expenses by category
            cat_result = await session.execute(
                select(Financial.category, func.sum(Financial.amount))
                .where(
                    Financial.date >= start,
                    Financial.date <= end,
                    Financial.type == "expense",
                )
                .group_by(Financial.category)
            )
            expense_by_category = {row[0]: float(row[1]) for row in cat_result}

        total_revenue = float(revenue) + float(fin_income)
        total_expenses = float(expenses) + float(commissions)
        net_profit = total_revenue - total_expenses
        margin_pct = round(net_profit / max(total_revenue, 1) * 100, 1)
        expense_ratio = round(total_expenses / max(total_revenue, 1), 3)

        return {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "total_revenue": round(total_revenue, 2),
            "total_expenses": round(total_expenses, 2),
            "net_profit": round(net_profit, 2),
            "margin_pct": margin_pct,
            "expense_ratio": expense_ratio,
            "expense_by_category": expense_by_category,
        }

    async def _get_claude_spend_today(self) -> float:
        """Get today's Claude API spend from agent logs."""
        today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
        async with get_session() as session:
            result = await session.scalar(
                select(func.sum(AgentLog.cost)).where(
                    AgentLog.action.like("claude_call:%"),
                    AgentLog.timestamp >= today_start,
                )
            )
        return float(result or 0)

    async def _format_daily_pnl(self, pnl: dict, claude_cost: float, target_date: date) -> str:
        """Format P&L as Telegram report using Sonnet."""
        cat_text = "\n".join(
            f"  - {k}: {v:.0f} руб."
            for k, v in pnl.get("expense_by_category", {}).items()
        ) or "  - нет расходов"

        prompt = f"""Напиши краткий ежедневный P&L отчёт для владельца бизнеса.

Данные за {target_date.isoformat()}:
- Выручка: {pnl['total_revenue']:.0f} руб.
- Расходы: {pnl['total_expenses']:.0f} руб.
- Чистая прибыль: {pnl['net_profit']:.0f} руб.
- Маржа: {pnl['margin_pct']}%
- Расходы на AI (Claude): {claude_cost:.3f} USD

Расходы по категориям:
{cat_text}

Формат: Telegram Markdown, сжато, на русском, с эмодзи. До 350 символов.
Если прибыль отрицательная — добавь предупреждение."""

        try:
            report = await self.call_sonnet(
                system="Ты — бухгалтер. Пишешь точные финансовые отчёты.",
                user=prompt,
                max_tokens=450,
            )
            return f"💰 *P&L {target_date.isoformat()}*\n\n{report}"
        except Exception:
            profit_emoji = "📈" if pnl["net_profit"] >= 0 else "📉"
            return (
                f"💰 *P&L {target_date.isoformat()}*\n"
                f"Выручка: {pnl['total_revenue']:.0f} ₽ | "
                f"Расходы: {pnl['total_expenses']:.0f} ₽ | "
                f"{profit_emoji} Прибыль: {pnl['net_profit']:.0f} ₽ ({pnl['margin_pct']}%)"
            )

    # ── Weekly summary ─────────────────────────────────────────

    async def _weekly_summary(self) -> dict:
        today = date.today()
        week_start = today - timedelta(days=7)
        pnl = await self._calculate_pnl(week_start, today)
        claude_cost = await self._get_claude_weekly_spend()

        report = await self._format_weekly_summary(pnl, claude_cost, week_start, today)
        await self.report_to_telegram(report)

        await self.log(
            action="weekly_summary",
            result=f"revenue={pnl['total_revenue']} profit={pnl['net_profit']}",
        )
        return {"status": "ok", "pnl": pnl}

    async def _get_claude_weekly_spend(self) -> float:
        week_start = datetime.combine(
            date.today() - timedelta(days=7), datetime.min.time()
        ).replace(tzinfo=timezone.utc)
        async with get_session() as session:
            result = await session.scalar(
                select(func.sum(AgentLog.cost)).where(
                    AgentLog.action.like("claude_call:%"),
                    AgentLog.timestamp >= week_start,
                )
            )
        return float(result or 0)

    async def _format_weekly_summary(
        self, pnl: dict, claude_cost: float, week_start: date, week_end: date
    ) -> str:
        prompt = f"""Напиши еженедельный финансовый отчёт.

Период: {week_start.isoformat()} — {week_end.isoformat()}
Выручка: {pnl['total_revenue']:.0f} руб.
Расходы: {pnl['total_expenses']:.0f} руб.
Чистая прибыль: {pnl['net_profit']:.0f} руб.
Маржа: {pnl['margin_pct']}%
Расходы на AI: {claude_cost:.2f} USD (бюджет $300/мес)
Доля расходов: {pnl['expense_ratio'] * 100:.1f}% от выручки (лимит 60%)

Дай 2-3 вывода о финансовом здоровье бизнеса. Telegram Markdown, русский язык, до 400 символов."""

        try:
            summary = await self.call_sonnet(
                system="Ты — финансовый аналитик. Краткие выводы для предпринимателя.",
                user=prompt,
                max_tokens=500,
            )
            return f"📋 *Недельный отчёт {week_start} — {week_end}*\n\n{summary}"
        except Exception:
            return (
                f"📋 *Неделя {week_start} — {week_end}*\n"
                f"Выручка: {pnl['total_revenue']:.0f} ₽ | "
                f"Прибыль: {pnl['net_profit']:.0f} ₽ | "
                f"Маржа: {pnl['margin_pct']}%"
            )

    # ── Monthly report ─────────────────────────────────────────

    async def _monthly_report(self) -> dict:
        today = date.today()
        # Previous month
        first_day = today.replace(day=1)
        last_month_end = first_day - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)

        pnl = await self._calculate_pnl(last_month_start, last_month_end)
        product_breakdown = await self._product_breakdown(last_month_start, last_month_end)
        claude_monthly = await self._get_monthly_claude_spend()

        report = await self._format_monthly_report(
            pnl, product_breakdown, claude_monthly, last_month_start, last_month_end
        )
        await self.report_to_telegram(report)

        # Tax flag
        if pnl["total_revenue"] > 0:
            await self.report_to_telegram(
                f"⚠️ *Налоговое напоминание*\n"
                f"Выручка за месяц: {pnl['total_revenue']:.0f} руб.\n"
                f"Не забудьте отразить в декларации (УСН / самозанятость)."
            )

        return {"status": "ok", "pnl": pnl}

    async def _product_breakdown(self, date_from: date, date_to: date) -> list[dict]:
        """P&L breakdown by product category."""
        start = datetime.combine(date_from, datetime.min.time()).replace(tzinfo=timezone.utc)
        end = datetime.combine(date_to, datetime.max.time()).replace(tzinfo=timezone.utc)
        async with get_session() as session:
            result = await session.execute(
                select(Financial.category, func.sum(Financial.amount), Financial.type)
                .where(Financial.date >= start, Financial.date <= end)
                .group_by(Financial.category, Financial.type)
            )
            rows = result.all()

        breakdown: dict[str, dict] = {}
        for cat, amount, ftype in rows:
            if cat not in breakdown:
                breakdown[cat] = {"income": 0.0, "expense": 0.0}
            breakdown[cat][ftype] = breakdown[cat].get(ftype, 0) + float(amount)

        return [
            {"category": k, **v, "profit": v.get("income", 0) - v.get("expense", 0)}
            for k, v in breakdown.items()
        ]

    async def _get_monthly_claude_spend(self) -> float:
        from core.base_agent import _month_start
        async with get_session() as session:
            result = await session.scalar(
                select(func.sum(AgentLog.cost)).where(
                    AgentLog.action.like("claude_call:%"),
                    AgentLog.timestamp >= _month_start(),
                )
            )
        return float(result or 0)

    async def _format_monthly_report(
        self,
        pnl: dict,
        product_breakdown: list[dict],
        claude_spend: float,
        month_start: date,
        month_end: date,
    ) -> str:
        breakdown_text = "\n".join(
            f"  - {b['category']}: доход {b.get('income', 0):.0f} руб., прибыль {b['profit']:.0f} руб."
            for b in product_breakdown[:6]
        ) or "  нет данных"

        prompt = f"""Напиши полный ежемесячный финансовый отчёт.

Период: {month_start.strftime('%B %Y')}
Выручка: {pnl['total_revenue']:.0f} руб.
Расходы: {pnl['total_expenses']:.0f} руб.
Чистая прибыль: {pnl['net_profit']:.0f} руб.
Рентабельность: {pnl['margin_pct']}%
Доля расходов: {pnl['expense_ratio'] * 100:.1f}%

Разбивка по категориям:
{breakdown_text}

Расходы на AI-агентов (Claude): {claude_spend:.2f} USD из $300 бюджета

Включи:
1. Итог месяца (1 предложение)
2. Топ-2 вывода о рентабельности
3. Главная рекомендация на следующий месяц

Telegram Markdown, русский язык, до 600 символов."""

        try:
            report = await self.call_sonnet(
                system="Ты — CFO малого бизнеса. Точный и конкретный финансовый анализ.",
                user=prompt,
                max_tokens=700,
            )
            return f"📊 *Месячный отчёт — {month_start.strftime('%B %Y')}*\n\n{report}"
        except Exception:
            return (
                f"📊 *Отчёт {month_start.strftime('%B %Y')}*\n"
                f"Выручка: {pnl['total_revenue']:.0f} ₽\n"
                f"Прибыль: {pnl['net_profit']:.0f} ₽ ({pnl['margin_pct']}%)\n"
                f"AI-расходы: ${claude_spend:.2f}"
            )

    # ── Record transaction ─────────────────────────────────────

    async def _record_transaction(self, task: dict) -> dict:
        """Manually record a financial transaction."""
        required = ["type", "amount", "category", "description"]
        self.validate_schema(task, required, "accountant.record_transaction")

        async with get_session() as session:
            f = Financial(
                type=task["type"],
                amount=Decimal(str(task["amount"])),
                category=task["category"],
                description=task["description"],
                agent_source=task.get("agent_source", "manual"),
            )
            session.add(f)
            await session.flush()
            fin_id = f.id

        await self.log(action="record_transaction", result=f"id={fin_id} {task['type']}={task['amount']}")
        return {"status": "ok", "financial_id": fin_id}

    # ── Expense ratio check ────────────────────────────────────

    async def _expense_ratio_check(self) -> dict:
        """Alert if monthly expenses exceed threshold% of revenue."""
        today = date.today()
        month_start = today.replace(day=1)
        pnl = await self._calculate_pnl(month_start, today)

        if pnl["expense_ratio"] >= settings.expense_alert_threshold:
            await self.report_to_telegram(
                f"🚨 *Бухгалтер — Превышение расходов!*\n"
                f"Расходы составляют *{pnl['expense_ratio'] * 100:.1f}%* от выручки\n"
                f"(лимит: {settings.expense_alert_threshold * 100:.0f}%)\n"
                f"Выручка: {pnl['total_revenue']:.0f} руб. | "
                f"Расходы: {pnl['total_expenses']:.0f} руб."
            )

        return {"status": "ok", "expense_ratio": pnl["expense_ratio"]}
