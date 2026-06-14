# agents/procurement.py
"""
Agent 7 — Procurement Agent
Mission: Decide what to buy, from whom, at what price.
Minimum acceptable margin: 40%
Never recommend purchase without unit economics.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx
import structlog
from sqlalchemy import select, update

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Financial, Inventory, Product, Trend, get_session

log = structlog.get_logger("procurement")

# Supplier regions with typical cost multipliers vs. retail price
SUPPLIER_PROFILES = {
    "china": {"lead_days": 21, "min_margin_uplift": 0.0, "reliability": "high"},
    "turkey": {"lead_days": 14, "min_margin_uplift": 0.05, "reliability": "very_high"},
    "russia_local": {"lead_days": 3, "min_margin_uplift": -0.10, "reliability": "medium"},
}

CATEGORY_COST_BENCHMARKS = {
    # (min_cost_rub, max_cost_rub) for sourcing from China
    "hoodie": (800, 1800),
    "t-shirt": (350, 900),
    "cap": (250, 700),
    "pants": (700, 1600),
}


class ProcurementAgent(BaseAgent):
    name = "procurement"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        if trigger == "weekly_review":
            return await self._weekly_review()
        elif trigger == "inventory_check":
            return await self._inventory_check()
        elif trigger == "evaluate_trend":
            return await self._evaluate_trend(task["trend_id"])
        else:
            return {"status": "ok"}

    # ── Weekly review ──────────────────────────────────────────

    async def _weekly_review(self) -> dict:
        """Pull HOT trends, calculate unit economics, generate purchase orders."""
        # Load approved trends not yet ordered
        async with get_session() as session:
            result = await session.execute(
                select(Trend).where(
                    Trend.status == "new",
                    Trend.score >= settings.trend_score_threshold,
                ).order_by(Trend.score.desc()).limit(5)
            )
            trends = result.scalars().all()

        if not trends:
            self._log.info("procurement.no_new_trends")
            return {"status": "ok", "purchase_orders": []}

        purchase_orders = []
        for trend in trends:
            order = await self._evaluate_trend(trend.id)
            if order.get("recommended"):
                purchase_orders.append(order)

        if purchase_orders:
            report = await self._format_procurement_report(purchase_orders)
            await self.report_to_telegram(report)

        await self.log(
            action="weekly_review",
            result=f"{len(purchase_orders)} purchase orders generated",
            confidence_score=0.9,
        )
        return {"status": "ok", "purchase_orders": purchase_orders}

    async def _evaluate_trend(self, trend_id: int) -> dict:
        """Full unit economics evaluation for one trend."""
        async with get_session() as session:
            result = await session.execute(
                select(Trend).where(Trend.id == trend_id)
            )
            trend = result.scalar_one_or_none()

        if not trend:
            return {"status": "error", "error": "trend not found"}

        # ── Step 1: Estimate sourcing costs ───────────────────
        cost_data = await self._estimate_costs(trend)

        # ── Step 2: Calculate unit economics ──────────────────
        economics = self._calculate_unit_economics(trend, cost_data)

        # ── Step 3: Find suppliers ────────────────────────────
        suppliers = await self._find_suppliers(trend, cost_data)

        # ── Step 4: Sonnet decision ───────────────────────────
        decision = await self._make_purchase_decision(trend, economics, suppliers)

        # ── Step 5: If recommended, create product record ─────
        if decision.get("recommended"):
            product_id = await self._create_product_record(trend, economics, decision)
            decision["product_id"] = product_id

            # Record in financials as planned expense
            await self._record_planned_expense(trend, economics, decision)

            # Mark trend as ordered
            async with get_session() as session:
                await session.execute(
                    update(Trend).where(Trend.id == trend_id).values(status="ordered")
                )

        await self.log(
            action="evaluate_trend",
            result=f"trend={trend.name} recommended={decision.get('recommended')} margin={economics.get('margin_pct')}%",
            confidence_score=0.85,
        )
        return decision

    async def _estimate_costs(self, trend: Trend) -> dict:
        """Use Haiku to estimate sourcing cost ranges for this product."""
        category = trend.category or "hoodie"
        benchmark = CATEGORY_COST_BENCHMARKS.get(category, (500, 1500))

        prompt = f"""Ты — эксперт по закупкам одежды из Китая и Турции для российского рынка.

Товар: {trend.name} (категория: {category})
Ориентировочная себестоимость (Китай): {benchmark[0]}-{benchmark[1]} руб.

Оцени реалистичную закупочную цену и рекомендуемую розничную цену на Avito.
Учти: мин. маржа {settings.min_margin_percent}%, конкуренция на российском рынке.

Верни JSON:
{{
  "cost_china_rub": <число>,
  "cost_turkey_rub": <число>,
  "cost_russia_rub": <число>,
  "recommended_retail_rub": <число>,
  "avito_market_price_rub": <число>
}}"""

        try:
            result = await self.call_haiku_json(
                system="Ты — закупочный аналитик. Только реалистичные цифры. Только JSON.",
                user=prompt,
            )
            return result
        except Exception as exc:
            self._log.error("procurement.estimate_costs_error", error=str(exc))
            return {
                "cost_china_rub": benchmark[0],
                "cost_turkey_rub": int(benchmark[0] * 1.3),
                "cost_russia_rub": int(benchmark[1] * 1.2),
                "recommended_retail_rub": int(benchmark[1] * 2.5),
                "avito_market_price_rub": int(benchmark[1] * 2.2),
            }

    def _calculate_unit_economics(self, trend: Trend, cost_data: dict) -> dict:
        """Pure math — no AI needed for unit economics."""
        cost = float(cost_data.get("cost_china_rub", 1000))
        retail = float(cost_data.get("recommended_retail_rub", 2500))
        avito_commission = retail * 0.03      # Avito ~3% commission
        net_revenue = retail - avito_commission
        gross_profit = net_revenue - cost
        margin_pct = round(gross_profit / net_revenue * 100, 1) if net_revenue > 0 else 0

        # ROI on 10 units
        units = 10
        investment = cost * units
        total_profit = gross_profit * units
        roi = round(total_profit / investment * 100, 1) if investment > 0 else 0

        return {
            "cost_rub": cost,
            "retail_price_rub": retail,
            "avito_commission_rub": round(avito_commission, 2),
            "net_revenue_rub": round(net_revenue, 2),
            "gross_profit_rub": round(gross_profit, 2),
            "margin_pct": margin_pct,
            "roi_10units_pct": roi,
            "investment_10units_rub": investment,
            "viable": margin_pct >= settings.min_margin_percent,
        }

    async def _find_suppliers(self, trend: Trend, cost_data: dict) -> list[dict]:
        """Identify best supplier options with lead times and reliability."""
        suppliers = []
        for region, profile in SUPPLIER_PROFILES.items():
            cost_key = f"cost_{region}_rub"
            cost = float(cost_data.get(cost_key, cost_data.get("cost_china_rub", 1000)))
            retail = float(cost_data.get("recommended_retail_rub", 2500))
            margin = round((retail - cost) / retail * 100, 1)

            suppliers.append({
                "region": region,
                "estimated_cost_rub": cost,
                "lead_days": profile["lead_days"],
                "reliability": profile["reliability"],
                "margin_pct": margin,
                "viable": margin >= settings.min_margin_percent,
            })

        return sorted(suppliers, key=lambda x: x["margin_pct"], reverse=True)

    async def _make_purchase_decision(
        self, trend: Trend, economics: dict, suppliers: list[dict]
    ) -> dict:
        """Sonnet makes the final buy/no-buy decision with reasoning."""
        viable_suppliers = [s for s in suppliers if s["viable"]]

        prompt = f"""Ты — директор по закупкам интернет-магазина одежды на Avito.

Тренд: {trend.name} (оценка: {trend.score}/50)
Описание: {trend.recommendation or 'нет данных'}

Юнит-экономика:
{json.dumps(economics, ensure_ascii=False, indent=2)}

Поставщики:
{json.dumps(viable_suppliers if viable_suppliers else suppliers, ensure_ascii=False, indent=2)}

Правила принятия решений:
- Рекомендуй закупку только если маржа >= {settings.min_margin_percent}%
- Предпочти поставщика с лучшим балансом маржа/скорость
- Начальная партия: 10-20 единиц для тестирования
- Укажи конкретный порог для реордера

Верни JSON:
{{
  "recommended": true|false,
  "reason": "объяснение решения",
  "best_supplier": "china|turkey|russia_local",
  "order_quantity": <число>,
  "expected_margin_pct": <число>,
  "reorder_threshold": <число единиц>,
  "expected_revenue_rub": <число>,
  "risk_level": "low|medium|high",
  "color_primary": "цвет тренда",
  "color_secondary": "чёрный"
}}"""

        try:
            result = await self.call_sonnet_json(
                system="Ты — закупочный директор. Никогда не рекомендуй убыточные закупки. Только JSON.",
                user=prompt,
            )
            return result
        except Exception as exc:
            self._log.error("procurement.decision_error", error=str(exc))
            # Safe fallback: don't recommend if uncertain
            return {
                "recommended": False,
                "reason": f"Ошибка анализа: {exc}",
                "best_supplier": None,
                "order_quantity": 0,
            }

    async def _create_product_record(
        self, trend: Trend, economics: dict, decision: dict
    ) -> int:
        """Create Product record in DB for Designer/Copywriter to pick up."""
        async with get_session() as session:
            product = Product(
                trend_id=trend.id,
                name=trend.name,
                category=trend.category,
                color=decision.get("color_primary", "чёрный"),
                price_rub=Decimal(str(economics.get("retail_price_rub", 0))),
                cost_rub=Decimal(str(economics.get("cost_rub", 0))),
                margin=economics.get("margin_pct", 0),
                status="draft",
            )
            session.add(product)
            await session.flush()
            product_id = product.id

            # Create inventory record
            inventory = Inventory(
                product_id=product_id,
                quantity=0,
                reorder_threshold=decision.get("reorder_threshold", 5),
                supplier=decision.get("best_supplier", "china"),
            )
            session.add(inventory)
            return product_id

    async def _record_planned_expense(
        self, trend: Trend, economics: dict, decision: dict
    ) -> None:
        """Log the planned purchase as a financial transaction."""
        total = economics.get("cost_rub", 0) * decision.get("order_quantity", 0)
        async with get_session() as session:
            f = Financial(
                type="expense",
                amount=Decimal(str(total)),
                category="procurement",
                description=f"Закупка: {trend.name} x{decision.get('order_quantity')} ед. от {decision.get('best_supplier')}",
                agent_source="procurement",
            )
            session.add(f)

    async def _format_procurement_report(self, orders: list[dict]) -> str:
        """Format purchase orders as Telegram report."""
        lines = ["🛒 *Закупки — Рекомендации*\n"]
        for o in orders:
            lines.append(
                f"• *{o.get('recommended', False) and '✅' or '❌'}* "
                f"Маржа: {o.get('expected_margin_pct', '?')}% | "
                f"Кол-во: {o.get('order_quantity', '?')} ед. | "
                f"Риск: {o.get('risk_level', '?')}"
            )
            if o.get("reason"):
                lines.append(f"  _{o['reason'][:100]}_")
        return "\n".join(lines)

    # ── Inventory check ────────────────────────────────────────

    async def _inventory_check(self) -> dict:
        """Alert if any product stock drops below reorder threshold."""
        async with get_session() as session:
            result = await session.execute(
                select(Inventory, Product)
                .join(Product, Inventory.product_id == Product.id)
                .where(Product.status == "active")
            )
            rows = result.all()

        alerts = []
        for inv, product in rows:
            if inv.quantity <= inv.reorder_threshold:
                alerts.append({
                    "product_id": product.id,
                    "product_name": product.name,
                    "current_qty": inv.quantity,
                    "threshold": inv.reorder_threshold,
                    "supplier": inv.supplier,
                })

        if alerts:
            msg = "📦 *Низкий остаток! Нужен реордер:*\n\n"
            for a in alerts:
                msg += f"• {a['product_name']}: {a['current_qty']} ед. (порог: {a['threshold']}) → {a['supplier']}\n"
            await self.report_to_telegram(msg)

        self._log.info("procurement.inventory_check", low_stock_alerts=len(alerts))
        return {"status": "ok", "low_stock_alerts": len(alerts), "alerts": alerts}
