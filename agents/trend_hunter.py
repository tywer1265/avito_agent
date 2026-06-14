# agents/trend_hunter.py
"""
Agent 1 — Trend Hunter
Mission: Detect emerging clothing trends before they peak.
Schedule: Every Monday 09:00 MSK
Cost target: < $0.15 per weekly cycle
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import structlog
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Trend, get_session

log = structlog.get_logger("trend_hunter")

# Scoring weights (must sum to 50)
SCORE_WEIGHTS = {
    "momentum": 15,       # Is it growing?
    "avito_fit": 12,      # Will it sell in Russia?
    "supply_access": 8,   # Can we source it?
    "margin_potential": 8, # Is it profitable?
    "timing_window": 7,   # How much runway left?
}

CATEGORIES = ["hoodie", "t-shirt", "cap", "pants"]


class TrendHunterAgent(BaseAgent):
    name = "trend_hunter"

    async def execute(self, task: dict) -> dict:
        self._log.info("trend_hunter.execute.start", task=task)
        raw_signals: list[dict] = []

        # ── Step 1: Gather signals from all sources ────────────
        results = await asyncio.gather(
            self._fetch_newsapi_trends(),
            self._fetch_wildberries_bestsellers(),
            self._fetch_vk_trends(),
            self._fetch_avito_trends(),
            return_exceptions=True,
        )

        source_names = ["newsapi", "wildberries", "vkontakte", "avito"]
        for name, result in zip(source_names, results):
            if isinstance(result, Exception):
                self._log.error("trend_hunter.source_error", source=name, error=str(result))
                await self.report_to_telegram(f"⚠️ Trend Hunter: source `{name}` failed — {result}")
            else:
                raw_signals.extend(result)

        if not raw_signals:
            await self.report_to_telegram("🚨 Trend Hunter: ALL sources failed. No data this week.")
            return {"status": "error", "trends": []}

        # ── Step 2: Haiku deduplication & classification ───────
        classified = await self._classify_signals(raw_signals)

        # ── Step 3: Filter — valid only if detected in 2+ sources ─
        valid_trends = [t for t in classified if t.get("source_count", 0) >= settings.trend_min_sources]
        self._log.info("trend_hunter.filter", total=len(classified), valid=len(valid_trends))

        # ── Step 4: Sonnet scores & final report ───────────────
        if not valid_trends:
            msg = "📊 Trend Hunter: no multi-source trends found this week."
            await self.report_to_telegram(msg)
            return {"status": "ok", "trends": [], "message": msg}

        scored_trends = await self._score_trends(valid_trends)
        top_10 = sorted(scored_trends, key=lambda x: x["score"], reverse=True)[:10]

        # ── Step 5: Save to DB ─────────────────────────────────
        saved_ids = await self._save_trends(top_10)

        # ── Step 6: Final report → Telegram ───────────────────
        report = await self._generate_report(top_10)
        await self.report_to_telegram(report)

        await self.log(
            action="weekly_trend_scan",
            result=f"{len(top_10)} trends found",
            confidence_score=0.85,
            input_summary=f"{len(raw_signals)} raw signals",
        )

        return {"status": "ok", "trends": top_10, "saved_ids": saved_ids}

    # ── Source fetchers ────────────────────────────────────────

    async def _fetch_newsapi_trends(self) -> list[dict]:
        """Fetch fashion/clothing news from NewsAPI."""
        signals = []
        queries = ["Russian fashion trends", "одежда тренд 2025", "streetwear Russia"]
        async with httpx.AsyncClient(timeout=15) as client:
            for q in queries:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(min=2, max=10),
                    retry=retry_if_exception_type(httpx.HTTPError),
                    reraise=True,
                ):
                    with attempt:
                        resp = await client.get(
                            f"{settings.news_api_base_url}/everything",
                            params={
                                "q": q,
                                "language": "ru",
                                "sortBy": "publishedAt",
                                "pageSize": 10,
                                "apiKey": settings.news_api_key,
                                "from": (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
                            },
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        for article in data.get("articles", []):
                            signals.append({
                                "source": "newsapi",
                                "text": f"{article.get('title', '')} {article.get('description', '')}",
                                "url": article.get("url"),
                                "published_at": article.get("publishedAt"),
                            })
        return signals

    async def _fetch_wildberries_bestsellers(self) -> list[dict]:
        """Scrape Wildberries bestseller categories for clothing items."""
        signals = []
        wb_categories = [
            ("Толстовки и свитшоты", 8146),
            ("Футболки", 458182),
            ("Кепки и бейсболки", 92140),
            ("Брюки", 61601),
        ]
        async with httpx.AsyncClient(timeout=15, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for cat_name, cat_id in wb_categories:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(min=2, max=10),
                    retry=retry_if_exception_type(httpx.HTTPError),
                    reraise=True,
                ):
                    with attempt:
                        resp = await client.get(
                            f"{settings.wb_api_base_url}/catalog/v2/filters",
                            params={
                                "appType": 1,
                                "curr": "rub",
                                "dest": -1257786,
                                "sort": "popular",
                                "subject": cat_id,
                                "limit": 20,
                            },
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        products = data.get("data", {}).get("products", [])
                        for p in products[:10]:
                            signals.append({
                                "source": "wildberries",
                                "text": f"{cat_name}: {p.get('name', '')} brand:{p.get('brand', '')}",
                                "sales_rank": products.index(p),
                                "category": cat_name,
                            })
        return signals

    async def _fetch_vk_trends(self) -> list[dict]:
        """Fetch trending posts from VK fashion communities."""
        signals = []
        vk_groups = [
            "fashionrussia",
            "streetwear_russia",
            "avito_style",
        ]
        async with httpx.AsyncClient(timeout=15) as client:
            for group in vk_groups:
                try:
                    async for attempt in AsyncRetrying(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(min=2, max=10),
                        retry=retry_if_exception_type(httpx.HTTPError),
                        reraise=True,
                    ):
                        with attempt:
                            resp = await client.get(
                                "https://api.vk.com/method/wall.get",
                                params={
                                    "domain": group,
                                    "count": 10,
                                    "access_token": settings.vk_service_token,
                                    "v": settings.vk_api_version,
                                    "filter": "owner",
                                },
                            )
                            resp.raise_for_status()
                            data = resp.json()
                            items = data.get("response", {}).get("items", [])
                            for post in items:
                                text = post.get("text", "")
                                likes = post.get("likes", {}).get("count", 0)
                                reposts = post.get("reposts", {}).get("count", 0)
                                if likes + reposts > 50 and text:
                                    signals.append({
                                        "source": "vkontakte",
                                        "text": text[:300],
                                        "engagement": likes + reposts,
                                        "group": group,
                                    })
                except Exception as exc:
                    self._log.warning("trend_hunter.vk_group_error", group=group, error=str(exc))
        return signals

    async def _fetch_avito_trends(self) -> list[dict]:
        """
        Use Avito API to see which clothing items have highest search volume/views.
        Falls back to category stats if search trends endpoint unavailable.
        """
        signals = []
        search_queries = ["худи", "футболка", "кепка", "штаны", "свитшот"]
        token = await self._get_avito_token()
        if not token:
            return signals

        async with httpx.AsyncClient(timeout=15) as client:
            for q in search_queries:
                try:
                    resp = await client.get(
                        f"{settings.avito_api_base_url}/core/v1/items",
                        headers={"Authorization": f"Bearer {token}"},
                        params={
                            "query": q,
                            "category_id": 1,  # clothing
                            "sort_by": "date",
                            "limit": 20,
                        },
                    )
                    if resp.status_code == 200:
                        items = resp.json().get("items", [])
                        for item in items[:10]:
                            signals.append({
                                "source": "avito",
                                "text": f"{item.get('title', '')} {item.get('category', {}).get('name', '')}",
                                "price": item.get("price_string"),
                                "views": item.get("stats", {}).get("views", 0),
                            })
                except Exception as exc:
                    self._log.warning("trend_hunter.avito_fetch_error", query=q, error=str(exc))
        return signals

    async def _get_avito_token(self) -> str | None:
        """Fetch OAuth2 token from Avito."""
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
            self._log.error("trend_hunter.avito_token_error", error=str(exc))
            return None

    # ── AI analysis steps ──────────────────────────────────────

    async def _classify_signals(self, signals: list[dict]) -> list[dict]:
        """
        Use Haiku to group signals into clothing trends and count source diversity.
        Cheap: one batch call.
        """
        signal_texts = [
            {"id": i, "source": s["source"], "text": s["text"][:150]}
            for i, s in enumerate(signals[:80])  # cap to control tokens
        ]
        prompt = f"""You are a clothing trend analyst for the Russian market.
Given these raw signals from multiple sources, identify distinct clothing trends.
Group related signals together. Count how many unique sources mention each trend.
Focus only on: hoodies, t-shirts, caps, pants and related clothing items.

Signals:
{json.dumps(signal_texts, ensure_ascii=False)}

Return JSON:
{{
  "trends": [
    {{
      "name": "trend name in Russian",
      "category": "hoodie|t-shirt|cap|pants",
      "source_count": <number of unique sources>,
      "sources": ["newsapi", "wildberries", ...],
      "description": "brief description",
      "signal_ids": [list of signal ids]
    }}
  ]
}}"""
        try:
            result = await self.call_haiku_json(
                system="You are a trend classification engine. Return only valid JSON.",
                user=prompt,
            )
            return result.get("trends", [])
        except Exception as exc:
            self._log.error("trend_hunter.classify_error", error=str(exc))
            return []

    async def _score_trends(self, trends: list[dict]) -> list[dict]:
        """
        Use Sonnet to score each trend on the 5-dimension rubric (max 50 points).
        """
        prompt = f"""You are a Russian clothing market expert scoring trends for an Avito seller.
Score each trend on these dimensions:
- momentum (0-15): Is it growing rapidly?
- avito_fit (0-12): Will Russian Avito buyers purchase this?
- supply_access (0-8): Can a small Russian seller source this cheaply from China/Turkey?
- margin_potential (0-8): Can it be sold at 40%+ margin?
- timing_window (0-7): Is there still time to capitalise (not already peaked)?

Trends to score:
{json.dumps(trends, ensure_ascii=False, indent=2)}

Return JSON:
{{
  "scored_trends": [
    {{
      "name": "...",
      "category": "...",
      "source_count": <n>,
      "sources": [...],
      "description": "...",
      "scores": {{
        "momentum": <0-15>,
        "avito_fit": <0-12>,
        "supply_access": <0-8>,
        "margin_potential": <0-8>,
        "timing_window": <0-7>
      }},
      "score": <total 0-50>,
      "recommendation": "brief procurement recommendation in Russian"
    }}
  ]
}}"""
        try:
            result = await self.call_sonnet_json(
                system="You are a market analysis engine. Return only valid JSON. Never fabricate data.",
                user=prompt,
            )
            return result.get("scored_trends", [])
        except Exception as exc:
            self._log.error("trend_hunter.score_error", error=str(exc))
            return trends  # return unscored if fails

    async def _generate_report(self, trends: list[dict]) -> str:
        """Generate a Telegram-ready Markdown report using Sonnet."""
        brief = json.dumps(
            [{"name": t["name"], "score": t.get("score", 0), "rec": t.get("recommendation", "")}
             for t in trends],
            ensure_ascii=False,
        )
        prompt = f"""Write a concise weekly trend report for an Avito clothing business owner.
Top trends this week (scored out of 50):
{brief}

Format as Telegram Markdown. Include:
1. Quick summary sentence
2. Top 3 actionable recommendations
3. One trend to AVOID and why
Keep under 500 characters total. Write in Russian."""

        try:
            report = await self.call_sonnet(
                system="You are a business analyst. Write concise Russian Telegram reports.",
                user=prompt,
                max_tokens=600,
            )
            return f"📈 *Тренды недели — Trend Hunter*\n\n{report}"
        except Exception as exc:
            self._log.error("trend_hunter.report_error", error=str(exc))
            lines = [f"📈 *Тренды недели*\n"]
            for t in trends[:5]:
                lines.append(f"• {t['name']} — {t.get('score', 0)}/50")
            return "\n".join(lines)

    async def _save_trends(self, trends: list[dict]) -> list[int]:
        """Persist top trends to DB."""
        ids = []
        threshold = settings.trend_score_threshold
        async with get_session() as session:
            for t in trends:
                if t.get("score", 0) >= threshold:
                    trend = Trend(
                        name=t["name"],
                        category=t.get("category"),
                        score=t.get("score", 0),
                        sources=json.dumps(t.get("sources", []), ensure_ascii=False),
                        recommendation=t.get("recommendation"),
                        status="new",
                    )
                    session.add(trend)
                    await session.flush()
                    ids.append(trend.id)
        return ids
