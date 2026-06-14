# agents/copywriter.py
"""
Agent 3 — Copywriter
Mission: Write high-converting listing texts for Avito in Russian.
Cost target: < $0.05 per listing
"""
from __future__ import annotations

import json
from typing import Optional

import httpx
import structlog
from sqlalchemy import select, update

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Listing, Product, get_session

log = structlog.get_logger("copywriter")

# A/B test tracking: title variant → conversion count
_AB_RESULTS: dict[str, dict] = {}


class CopywriterAgent(BaseAgent):
    name = "copywriter"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        if trigger == "pending_listings_sweep":
            return await self._process_pending_products()
        elif trigger == "write_for_product":
            return await self._write_listing(task["product_id"])
        else:
            return {"status": "ok"}

    async def _process_pending_products(self) -> dict:
        """Find products with images_ready status but no listing draft."""
        async with get_session() as session:
            result = await session.execute(
                select(Product).where(Product.status == "images_ready").limit(10)
            )
            products = result.scalars().all()

        if not products:
            return {"status": "ok", "processed": 0}

        results = []
        for product in products:
            r = await self._write_listing(product.id)
            results.append(r)

        return {"status": "ok", "processed": len(results), "results": results}

    async def _write_listing(self, product_id: int) -> dict:
        product = await self._load_product(product_id)
        if not product:
            return {"status": "error", "error": f"Product {product_id} not found"}

        # Fetch competitor data for benchmarking
        competitor_data = await self._get_competitor_data(product)

        # Generate listing copy
        listing_data = await self._generate_copy(product, competitor_data)

        # Generate A/B variant title
        ab_title = await self._generate_ab_title(product, listing_data["title"])

        # Save listing draft to DB
        listing_id = await self._save_listing(product_id, listing_data, ab_title)

        await self.log(
            action="write_listing",
            result=f"listing_id={listing_id} title={listing_data['title'][:40]}",
            confidence_score=0.88,
            input_summary=f"product_id={product_id}",
        )

        # Mark product as listing_ready
        async with get_session() as session:
            await session.execute(
                update(Product).where(Product.id == product_id).values(status="listing_ready")
            )

        return {
            "status": "ok",
            "listing_id": listing_id,
            "title": listing_data["title"],
            "ab_title": ab_title,
        }

    async def _get_competitor_data(self, product: Product) -> str:
        """Lightweight competitor scrape — just category search on Avito."""
        try:
            token = await self._get_avito_token()
            if not token:
                return "Данные конкурентов недоступны."
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{settings.avito_api_base_url}/core/v1/items",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"query": product.name, "limit": 5},
                )
                if resp.status_code != 200:
                    return "Данные конкурентов недоступны."
                items = resp.json().get("items", [])
                lines = []
                for item in items:
                    lines.append(
                        f"Цена: {item.get('price_string', '?')} — {item.get('title', '')[:60]}"
                    )
                return "Конкуренты:\n" + "\n".join(lines)
        except Exception as exc:
            self._log.warning("copywriter.competitor_fetch_error", error=str(exc))
            return "Данные конкурентов недоступны."

    async def _generate_copy(self, product: Product, competitor_data: str) -> dict:
        """Use Sonnet to write full listing copy."""
        prompt = f"""Ты — профессиональный копирайтер для маркетплейса Avito (Россия).
Напиши продающий текст для объявления о продаже одежды.

Товар:
- Название: {product.name}
- Категория: {product.category}
- Цвет: {product.color}
- Цена: {product.price_rub} руб.
- Себестоимость: {product.cost_rub} руб. (не указывай покупателю)

{competitor_data}

Требования:
1. Заголовок: максимум 50 символов, содержит ключевые слова для поиска Avito, никаких эмодзи в заголовке
2. Описание: 300-500 символов, психология покупателя, выгоды, социальное доказательство
3. Ключевые характеристики: 4-5 пунктов (материал, размеры, стиль, уход)
4. Рекомендованная цена с обоснованием

Только на русском языке.

Верни JSON:
{{
  "title": "...",
  "description": "...",
  "specs": ["...", "..."],
  "price_recommendation": <число>,
  "price_reasoning": "..."
}}"""

        result = await self.call_sonnet_json(
            system="Ты — эксперт по продажам на Avito. Возвращай только валидный JSON.",
            user=prompt,
        )
        # Validate
        self.validate_schema(result, ["title", "description", "specs"], "copywriter.generate")
        # Enforce title length
        if len(result["title"]) > 50:
            result["title"] = result["title"][:50]
        return result

    async def _generate_ab_title(self, product: Product, primary_title: str) -> str:
        """Use Haiku to generate an A/B test variant of the title."""
        prompt = f"""Напиши альтернативный заголовок объявления Avito для A/B теста.
Оригинальный заголовок: "{primary_title}"
Товар: {product.name}, цена {product.price_rub} руб.

Правила:
- Максимум 50 символов
- Используй другой подход (другие ключевые слова или структуру)
- Только заголовок, никакого другого текста"""

        try:
            ab = await self.call_haiku(
                system="Ты — копирайтер для Avito. Только текст заголовка.",
                user=prompt,
                max_tokens=80,
            )
            ab = ab.strip().strip('"')[:50]
            return ab
        except Exception:
            return primary_title  # fallback to primary if fails

    async def _save_listing(
        self, product_id: int, listing_data: dict, ab_title: str
    ) -> int:
        async with get_session() as session:
            listing = Listing(
                product_id=product_id,
                title=listing_data["title"],
                description=json.dumps(
                    {
                        "primary": listing_data["description"],
                        "specs": listing_data.get("specs", []),
                        "ab_title": ab_title,
                        "price_recommendation": listing_data.get("price_recommendation"),
                        "price_reasoning": listing_data.get("price_reasoning"),
                    },
                    ensure_ascii=False,
                ),
                status="draft",
            )
            session.add(listing)
            await session.flush()
            return listing.id

    async def _load_product(self, product_id: int) -> Optional[Product]:
        async with get_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            return result.scalar_one_or_none()

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
            self._log.error("copywriter.avito_token_error", error=str(exc))
            return None

    # ── A/B Tracking (called externally by Publisher/Analyst) ──

    async def record_ab_result(self, listing_id: int, variant: str, event: str) -> None:
        """Record A/B test result. variant: 'primary'|'ab'. event: 'view'|'contact'."""
        key = f"{listing_id}_{variant}"
        if key not in _AB_RESULTS:
            _AB_RESULTS[key] = {"views": 0, "contacts": 0}
        if event in _AB_RESULTS[key]:
            _AB_RESULTS[key][event] += 1

    async def get_ab_winner(self, listing_id: int) -> str:
        """Return 'primary' or 'ab' based on higher contact/view ratio."""
        p_key = f"{listing_id}_primary"
        ab_key = f"{listing_id}_ab"
        p = _AB_RESULTS.get(p_key, {"views": 1, "contacts": 0})
        ab = _AB_RESULTS.get(ab_key, {"views": 1, "contacts": 0})
        p_ratio = p["contacts"] / max(p["views"], 1)
        ab_ratio = ab["contacts"] / max(ab["views"], 1)
        return "ab" if ab_ratio > p_ratio else "primary"
