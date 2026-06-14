# agents/sales_analyst.py
"""
Agent 6 — Sales Analyst
Mission: Track performance, optimize pricing, monitor competitors.
Reports: daily 20:00 MSK to Telegram.
Alerts: conversion drop > 30% immediately.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import httpx
import structlog
from sqlalchemy import func, select, text

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Financial, Listing, Order, Product, get_session

log = structlog.get_logger("sales_analyst")


class SalesAnalystAgent(BaseAgent):
    name = "sales_analyst"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        if trigger == "daily_report":
            return await self._daily_report()
        elif trigger == "competitor_scan":
            return await self._competitor_scan()
        elif trigger == "conversion_check":
            return await self._conversion_drop_check()
        elif trigger == "price_recommendations":
            return await self._generate_price_recommendations()
        else:
            return {"status": "ok"}

    # ── Daily report ───────────────────────────────────────────

    async def _daily_report(self) -> dict:
        today = date.today()
        stats = await self._gather_daily_stats(today)
        recommendations = await self._generate_price_recommendations()
        report = await self._format_daily_report(stats, recommendations)
        await self.report_to_telegram(report)

        await self.log(
            action="daily_report",
            result=f"views={stats.get('total_views')} revenue={stats.get('revenue_today')}",
            confidence_score=1.0,
        )
        return {"status": "ok", "stats": stats}

    async def _gather_daily_stats(self, target_date: date) -> dict:
        """Aggregate views, contacts, orders, revenue for today."""
        start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        async with get_session() as session:
            # Total active listings
            listing_count = await session.scalar(
                select(func.count()).where(Listing.status == "active")
            )

            # Views & contacts from all active listings
            view_result = await session.execute(
                select(func.sum(Listing.views), func.sum(Listing.contacts))
                .where(Listing.status == "active")
            )
            row = view_result.one()
            total_views = int(row[0] or 0)
            total_contacts = int(row[1] or 0)

            # Orders today
            orders_today = await session.scalar(
                select(func.count()).where(
                    Order.created_at >= start, Order.created_at < end
                )
            )

            # Revenue today
            revenue = await session.scalar(
                select(func.sum(Order.price)).where(
                    Order.created_at >= start,
                    Order.created_at < end,
                    Order.status.in_(["done", "confirmed"]),
                )
            )

            # Top listing by views
            top_listing = await session.execute(
                select(Listing).where(Listing.status == "active")
                .order_by(Listing.views.desc())
                .limit(1)
            )
            top = top_listing.scalar_one_or_none()

            # Conversion rate (contacts / views)
            conversion = round(total_contacts / max(total_views, 1) * 100, 2)

        return {
            "date": target_date.isoformat(),
            "active_listings": listing_count or 0,
            "total_views": total_views,
            "total_contacts": total_contacts,
            "conversion_rate_pct": conversion,
            "orders_today": orders_today or 0,
            "revenue_today": float(revenue or 0),
            "top_listing_title": top.title if top else "—",
            "top_listing_views": top.views if top else 0,
        }

    async def _format_daily_report(self, stats: dict, recommendations: dict) -> str:
        """Format daily stats as Telegram message using Sonnet."""
        recs = recommendations.get("recommendations", [])
        rec_text = "\n".join(f"• {r}" for r in recs[:3]) if recs else "Нет рекомендаций"

        prompt = f"""Напиши краткий ежедневный отчёт для владельца Avito магазина одежды.

Статистика за {stats['date']}:
- Активных объявлений: {stats['active_listings']}
- Просмотров: {stats['total_views']}
- Контактов: {stats['total_contacts']}
- Конверсия: {stats['conversion_rate_pct']}%
- Заказов: {stats['orders_today']}
- Выручка: {stats['revenue_today']:.0f} руб.
- Топ объявление: {stats['top_listing_title']} ({stats['top_listing_views']} просмотров)

Рекомендации:
{rec_text}

Формат: Telegram Markdown, краткий и по делу, на русском. Используй эмодзи умеренно. До 400 символов."""

        try:
            report = await self.call_sonnet(
                system="Ты — бизнес-аналитик. Пишешь краткие отчёты для предпринимателей.",
                user=prompt,
                max_tokens=500,
            )
            return f"📊 *Ежедневный отчёт — {stats['date']}*\n\n{report}"
        except Exception:
            return (
                f"📊 *Отчёт {stats['date']}*\n"
                f"Объявлений: {stats['active_listings']} | "
                f"Просмотров: {stats['total_views']} | "
                f"Конверсия: {stats['conversion_rate_pct']}% | "
                f"Выручка: {stats['revenue_today']:.0f} руб."
            )

    # ── Competitor scan ────────────────────────────────────────

    async def _competitor_scan(self) -> dict:
        """Check top 20 competitor listings and extract price/positioning data."""
        token = await self._get_avito_token()
        if not token:
            return {"status": "error", "error": "avito_token_failed"}

        categories = ["худи", "футболка", "кепка", "штаны"]
        competitor_data = []

        async with httpx.AsyncClient(timeout=15) as client:
            for category in categories:
                try:
                    resp = await client.get(
                        f"{settings.avito_api_base_url}/core/v1/items",
                        headers={"Authorization": f"Bearer {token}"},
                        params={
                            "query": category,
                            "sort_by": "date",
                            "limit": 5,
                        },
                    )
                    if resp.status_code == 200:
                        items = resp.json().get("items", [])
                        for item in items:
                            competitor_data.append({
                                "category": category,
                                "title": item.get("title", ""),
                                "price": item.get("price_string", ""),
                                "views": item.get("stats", {}).get("views", 0),
                            })
                except Exception as exc:
                    self._log.warning("analyst.competitor_scan_error", category=category, error=str(exc))

        # Analyse competitor data with Haiku
        if competitor_data:
            analysis = await self._analyse_competitors(competitor_data)
            await self.report_to_telegram(
                f"🔍 *Мониторинг конкурентов*\n\n{analysis[:600]}"
            )

        await self.log(action="competitor_scan", result=f"{len(competitor_data)} items scanned")
        return {"status": "ok", "competitors_scanned": len(competitor_data)}

    async def _analyse_competitors(self, data: list[dict]) -> str:
        prompt = f"""Проанализируй данные конкурентов на Avito (одежда):
{json.dumps(data[:20], ensure_ascii=False)}

Выдели:
1. Средние цены по категориям
2. Самые популярные товары (если есть данные о просмотрах)
3. Что стоит учесть в ценообразовании

Кратко, на русском, до 300 символов."""
        try:
            return await self.call_haiku(
                system="Ты — аналитик рынка e-commerce.",
                user=prompt,
                max_tokens=400,
            )
        except Exception:
            return "Данные конкурентов собраны. Анализ недоступен."

    # ── Conversion drop check ──────────────────────────────────

    async def _conversion_drop_check(self) -> dict:
        """Alert if any listing's conversion dropped > 30% vs. previous period."""
        alerts = []

        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(
                    Listing.status == "active",
                    Listing.views > 10,  # only listings with enough data
                )
            )
            listings = result.scalars().all()

        for listing in listings:
            if listing.views == 0:
                continue
            current_conv = listing.contacts / listing.views
            # Baseline: 5% conversion is normal for Avito clothing
            baseline_conv = 0.05
            if current_conv < baseline_conv * (1 - settings.conversion_drop_alert):
                alerts.append({
                    "listing_id": listing.id,
                    "title": listing.title,
                    "views": listing.views,
                    "contacts": listing.contacts,
                    "conversion": round(current_conv * 100, 2),
                })

        if alerts:
            alert_text = "⚠️ *Падение конверсии!*\n\n"
            for a in alerts[:5]:
                alert_text += (
                    f"• {a['title'][:40]}\n"
                    f"  Конверсия: {a['conversion']}% (просм: {a['views']}, контакты: {a['contacts']})\n"
                )
            await self.report_to_telegram(alert_text)

        self._log.info("analyst.conversion_check", alerts=len(alerts))
        return {"status": "ok", "conversion_alerts": len(alerts), "alerts": alerts}

    # ── Price recommendations ──────────────────────────────────

    async def _generate_price_recommendations(self) -> dict:
        """Generate pricing recommendations for active listings."""
        async with get_session() as session:
            result = await session.execute(
                select(Listing, Product)
                .join(Product, Listing.product_id == Product.id)
                .where(Listing.status == "active")
                .limit(20)
            )
            rows = result.all()

        if not rows:
            return {"recommendations": []}

        listing_data = []
        for listing, product in rows:
            conv = listing.contacts / max(listing.views, 1) * 100
            listing_data.append({
                "listing_id": listing.id,
                "title": listing.title[:40],
                "current_price": float(product.price_rub or 0),
                "cost": float(product.cost_rub or 0),
                "views": listing.views,
                "contacts": listing.contacts,
                "conversion_pct": round(conv, 1),
            })

        prompt = f"""Ты — ценовой аналитик для Avito магазина одежды.
Минимальная маржа: {settings.min_margin_percent}%.

Данные объявлений:
{json.dumps(listing_data, ensure_ascii=False)}

Дай рекомендации по ценообразованию:
- Если конверсия < 2% и просмотров > 50 → снизить цену
- Если конверсия > 10% → можно поднять цену
- Если просмотров мало → поднять в поиске (буст)
- Никогда не рекомендуй цену ниже cost / (1 - {settings.min_margin_percent / 100})

Верни JSON:
{{"recommendations": ["рекомендация 1", "рекомендация 2", ...], "price_actions": [{{"listing_id": N, "action": "raise|lower|boost|keep", "new_price": N_or_null}}]}}"""

        try:
            result = await self.call_sonnet_json(
                system="Ты — ценовой аналитик. Не выходи за рамки минимальной маржи. Только JSON.",
                user=prompt,
            )
            return result
        except Exception as exc:
            self._log.error("analyst.price_recommendations_error", error=str(exc))
            return {"recommendations": ["Ошибка получения рекомендаций"], "price_actions": []}

    # ── Helpers ───────────────────────────────────────────────

    async def _get_avito_token(self) -> Optional[str]:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{settings.avito_api_base_url}/token",
                    data={
                        "client_id": settings.avito_client_id,
                        "client_secret": settings.avito_client_secret,
                        "grant_type": "client_credentials",
                    },
                )
                resp.raise_for_status()
                return resp.json().get("access_token")
        except Exception as exc:
            self._log.error("analyst.avito_token_error", error=str(exc))
            return None
