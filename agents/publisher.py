# agents/publisher.py
"""
Agent 4 — Publisher
Mission: Post and manage all Avito listings.
Handles: publish, boost, price update, status monitoring, refresh.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog
from sqlalchemy import select, update
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Listing, get_session

log = structlog.get_logger("publisher")


class PublisherAgent(BaseAgent):
    name = "publisher"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        token = await self._get_avito_token()
        if not token:
            await self.report_to_telegram("🚨 Publisher: не удалось получить токен Avito!")
            return {"status": "error", "error": "avito_token_failed"}

        if trigger == "peak_hour_post":
            return await self._publish_pending_listings(token)
        elif trigger == "listing_refresh":
            return await self._refresh_active_listings(token)
        elif trigger == "price_update":
            return await self._update_listing_price(
                token, task["listing_id"], task["new_price"]
            )
        elif trigger == "boost_listing":
            return await self._boost_listing(token, task["listing_id"])
        elif trigger == "sync_stats":
            return await self._sync_listing_stats(token)
        else:
            return {"status": "ok"}

    # ── Publish new listings ───────────────────────────────────

    async def _publish_pending_listings(self, token: str) -> dict:
        """Post all draft listings to Avito."""
        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(Listing.status == "draft").limit(5)
            )
            listings = result.scalars().all()

        if not listings:
            self._log.info("publisher.no_pending_listings")
            return {"status": "ok", "published": 0}

        published = 0
        failed = 0
        for listing in listings:
            success = await self._publish_single(token, listing)
            if success:
                published += 1
            else:
                failed += 1

        msg = f"📤 Publisher: опубликовано {published} объявлений"
        if failed:
            msg += f", ошибок: {failed}"
        await self.report_to_telegram(msg)

        await self.log(
            action="publish_listings",
            result=f"published={published} failed={failed}",
            confidence_score=1.0,
        )
        return {"status": "ok", "published": published, "failed": failed}

    async def _publish_single(self, token: str, listing: Listing) -> bool:
        """Post one listing to Avito via API."""
        try:
            description_data = json.loads(listing.description or "{}")
            description_text = description_data.get("primary", listing.description or "")
            specs = description_data.get("specs", [])
            if specs:
                description_text += "\n\n" + "\n".join(f"• {s}" for s in specs)

            payload = {
                "category": {"id": 1},   # clothing category; refine per product
                "title": listing.title,
                "description": description_text,
                "price": 0,               # price pulled from product (set below)
                "status": "active",
                "address": {
                    "id": 637640,         # Moscow; configurable
                },
            }

            # Get price from product
            from core.database import Product
            async with get_session() as session:
                prod_result = await session.execute(
                    select(Product).where(Product.id == listing.product_id)
                )
                product = prod_result.scalar_one_or_none()
            if product:
                payload["price"] = int(product.price_rub or 0)

            async with httpx.AsyncClient(timeout=30) as client:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(min=2, max=10),
                    retry=retry_if_exception_type(httpx.HTTPError),
                    reraise=True,
                ):
                    with attempt:
                        resp = await client.post(
                            f"{settings.avito_api_base_url}/core/v1/accounts/{settings.avito_user_id}/items",
                            headers={
                                "Authorization": f"Bearer {token}",
                                "Content-Type": "application/json",
                            },
                            json=payload,
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        avito_id = str(data.get("id", ""))

            # Update listing in DB
            async with get_session() as session:
                await session.execute(
                    update(Listing)
                    .where(Listing.id == listing.id)
                    .values(
                        avito_id=avito_id,
                        status="active",
                        published_at=datetime.now(timezone.utc),
                    )
                )
            self._log.info("publisher.published", listing_id=listing.id, avito_id=avito_id)
            return True

        except Exception as exc:
            self._log.error("publisher.publish_failed", listing_id=listing.id, error=str(exc))
            async with get_session() as session:
                await session.execute(
                    update(Listing).where(Listing.id == listing.id).values(status="error")
                )
            return False

    # ── Price updates ──────────────────────────────────────────

    async def _update_listing_price(
        self, token: str, listing_id: int, new_price: int
    ) -> dict:
        listing = await self._load_listing(listing_id)
        if not listing or not listing.avito_id:
            return {"status": "error", "error": "listing not found or not published"}

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.patch(
                    f"{settings.avito_api_base_url}/core/v1/accounts/{settings.avito_user_id}/items/{listing.avito_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"price": new_price},
                )
                resp.raise_for_status()

            self._log.info("publisher.price_updated", listing_id=listing_id, new_price=new_price)
            await self.log(action="price_update", result=f"listing={listing_id} price={new_price}")
            return {"status": "ok", "listing_id": listing_id, "new_price": new_price}
        except Exception as exc:
            self._log.error("publisher.price_update_failed", error=str(exc))
            return {"status": "error", "error": str(exc)}

    # ── Listing boost ──────────────────────────────────────────

    async def _boost_listing(self, token: str, listing_id: int) -> dict:
        listing = await self._load_listing(listing_id)
        if not listing or not listing.avito_id:
            return {"status": "error", "error": "listing not found"}

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{settings.avito_api_base_url}/core/v1/accounts/{settings.avito_user_id}/items/{listing.avito_id}/vas",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"services": ["x2_1"]},  # Avito "x2 highlight" service
                )
                resp.raise_for_status()

            self._log.info("publisher.boosted", listing_id=listing_id)
            await self.log(action="boost_listing", result=f"listing={listing_id}")
            return {"status": "ok", "listing_id": listing_id}
        except Exception as exc:
            self._log.error("publisher.boost_failed", listing_id=listing_id, error=str(exc))
            return {"status": "error", "error": str(exc)}

    # ── Listing refresh ────────────────────────────────────────

    async def _refresh_active_listings(self, token: str) -> dict:
        """Re-activate or re-post listings to maintain search position."""
        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(Listing.status == "active").limit(20)
            )
            listings = result.scalars().all()

        refreshed = 0
        for listing in listings:
            if not listing.avito_id:
                continue
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(
                        f"{settings.avito_api_base_url}/core/v1/accounts/{settings.avito_user_id}/items/{listing.avito_id}/status",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"status": "active"},
                    )
                    if resp.status_code in (200, 204):
                        refreshed += 1
            except Exception as exc:
                self._log.warning("publisher.refresh_error", listing_id=listing.id, error=str(exc))

        self._log.info("publisher.refresh_done", refreshed=refreshed)
        return {"status": "ok", "refreshed": refreshed}

    # ── Stats sync ─────────────────────────────────────────────

    async def _sync_listing_stats(self, token: str) -> dict:
        """Pull views/contacts from Avito and update local DB."""
        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(
                    Listing.status == "active",
                    Listing.avito_id.isnot(None),
                ).limit(50)
            )
            listings = result.scalars().all()

        updated = 0
        for listing in listings:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        f"{settings.avito_api_base_url}/stats/v1/accounts/{settings.avito_user_id}/items",
                        headers={"Authorization": f"Bearer {token}"},
                        params={"item_ids": listing.avito_id, "date_from": "2024-01-01"},
                    )
                    if resp.status_code == 200:
                        stats = resp.json()
                        item_stats = stats.get("result", {}).get("items", [{}])[0]
                        views = item_stats.get("views", listing.views)
                        contacts = item_stats.get("contacts", listing.contacts)

                        async with get_session() as upd_session:
                            await upd_session.execute(
                                update(Listing)
                                .where(Listing.id == listing.id)
                                .values(views=views, contacts=contacts)
                            )
                        updated += 1
            except Exception as exc:
                self._log.warning("publisher.stats_sync_error", listing_id=listing.id, error=str(exc))

        return {"status": "ok", "stats_updated": updated}

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
            self._log.error("publisher.avito_token_error", error=str(exc))
            return None

    async def _load_listing(self, listing_id: int) -> Optional[Listing]:
        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(Listing.id == listing_id)
            )
            return result.scalar_one_or_none()
