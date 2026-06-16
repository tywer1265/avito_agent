# agents/client_manager.py
"""
Agent 5 — Client Manager
Mission: Handle all buyer messages 24/7, close sales.
Response time target: < 5 minutes

CHANGES:
- Added intent: purchase_confirmed
- Added _notify_sale() → POST to n8n webhook → Google Sheets
- Works for both Telegram (now) and Avito (in 5 days)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import structlog
from sqlalchemy import select, update

from core.base_agent import BaseAgent
from core.config import settings
from core.database import Listing, Message, Order, get_session

log = structlog.get_logger("client_manager")

MAX_DISCOUNT_PCT = 0.10
FOLLOW_UP_HOURS = 24
MAX_FOLLOW_UPS = 2

TOKEN_CACHE_FILE = os.path.join(os.path.dirname(__file__), "..", "avito_token_cache.json")

N8N_ORDERS_WEBHOOK = "https://tywer1265.app.n8n.cloud/webhook/orders/new"


class ClientManagerAgent(BaseAgent):
    name = "client_manager"

    async def execute(self, task: dict) -> dict:
        trigger = task.get("trigger")
        if trigger == "message_poll":
            return await self._poll_and_respond()
        elif trigger == "followup_sweep":
            return await self._send_followups()
        elif trigger == "collect_reviews":
            return await self._collect_reviews()
        else:
            return {"status": "ok"}

    # ── Token management ───────────────────────────────────────

    async def _get_avito_token(self) -> Optional[str]:
        cache = self._load_token_cache()
        refresh_token = cache.get("refresh_token") or os.getenv("AVITO_REFRESH_TOKEN")
        if not refresh_token:
            self._log.error("client_manager.no_refresh_token")
            return None

        expires_at = cache.get("expires_at", 0)
        if cache.get("access_token") and datetime.now().timestamp() < expires_at - 300:
            return cache["access_token"]

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{settings.avito_api_base_url}/token",
                    data={
                        "grant_type": "refresh_token",
                        "client_id": settings.avito_client_id,
                        "client_secret": settings.avito_client_secret,
                        "refresh_token": refresh_token,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                access_token = data.get("access_token")
                new_refresh = data.get("refresh_token", refresh_token)
                expires_in = data.get("expires_in", 86400)
                self._save_token_cache({
                    "access_token": access_token,
                    "refresh_token": new_refresh,
                    "expires_at": datetime.now().timestamp() + expires_in,
                })
                return access_token
        except Exception as exc:
            self._log.error("client_manager.token_refresh_error", error=str(exc))
            return None

    def _load_token_cache(self) -> dict:
        try:
            if os.path.exists(TOKEN_CACHE_FILE):
                with open(TOKEN_CACHE_FILE, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_token_cache(self, data: dict) -> None:
        try:
            with open(TOKEN_CACHE_FILE, "w") as f:
                json.dump(data, f)
        except Exception as exc:
            self._log.warning("client_manager.token_cache_save_error", error=str(exc))

    # ── Message polling ────────────────────────────────────────

    async def _poll_and_respond(self) -> dict:
        token = await self._get_avito_token()
        if not token:
            return {"status": "error", "error": "avito_token_failed"}

        messages = await self._fetch_avito_messages(token)
        if not messages:
            return {"status": "ok", "responded": 0}

        responded = 0
        escalated = 0
        sales = 0
        for msg in messages:
            saved_id = await self._save_inbound_message(msg)
            result = await self._handle_message(token, msg, saved_id)
            if result == "responded":
                responded += 1
            elif result == "escalated":
                escalated += 1
            elif result == "sale":
                responded += 1
                sales += 1

        self._log.info("client_manager.poll_done",
                       responded=responded, escalated=escalated, sales=sales)
        return {"status": "ok", "responded": responded,
                "escalated": escalated, "sales": sales}

    async def _fetch_avito_messages(self, token: str) -> list[dict]:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{settings.avito_api_base_url}/messenger/v2/accounts/{settings.avito_user_id}/chats",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"unread_only": True, "limit": 50},
                )
                resp.raise_for_status()
                chats = resp.json().get("chats", [])

                messages = []
                for chat in chats:
                    last_msg = chat.get("last_message", {})
                    if last_msg.get("direction") != "in":
                        continue
                    if last_msg.get("type") == "system":
                        continue

                    buyer = None
                    for user in chat.get("users", []):
                        if str(user.get("id")) != str(settings.avito_user_id):
                            buyer = user
                            break

                    messages.append({
                        "chat_id": chat.get("id"),
                        "buyer_id": buyer.get("id", "unknown") if buyer else "unknown",
                        "buyer_name": buyer.get("name", "Покупатель") if buyer else "Покупатель",
                        "text": last_msg.get("content", {}).get("text", ""),
                        "listing_id": chat.get("context", {}).get("value", {}).get("id"),
                        "listing_title": chat.get("context", {}).get("value", {}).get("title", ""),
                        "listing_price": chat.get("context", {}).get("value", {}).get("price_string", ""),
                    })
                return messages
        except Exception as exc:
            self._log.error("client_manager.fetch_messages_error", error=str(exc))
            return []

    async def _handle_message(self, token: str, msg: dict, saved_id: int) -> str:
        text = msg.get("text", "").strip()
        if not text:
            return "skipped"

        intent = await self._classify_intent(text)
        self._log.info("client_manager.intent", intent=intent, text=text[:80])

        if intent in ("dispute", "complaint_complex"):
            await self._escalate(msg, intent)
            await self._update_message_status(saved_id, "escalated")
            return "escalated"

        if intent == "price_negotiation":
            reply = await self._handle_negotiation(msg)
        else:
            reply = await self._generate_reply(msg, intent)

        if not reply:
            return "skipped"

        success = await self._send_reply(token, msg["chat_id"], reply)
        if success:
            await self._save_outbound_message(msg, reply, saved_id)
            await self._update_message_status(saved_id, "replied")

            # Фиксируем продажу → n8n → Sheets
            if intent == "purchase_confirmed":
                await self._notify_sale(msg)
                return "sale"

            return "responded"
        return "skipped"

    async def _classify_intent(self, text: str) -> str:
        prompt = f"""Классифицируй сообщение покупателя на Avito (одежда):
"{text}"

Варианты:
- greeting: приветствие, общий вопрос
- availability: вопрос о наличии товара
- size_question: вопрос о размере
- price_negotiation: торг, просьба о скидке
- shipping_question: вопрос о доставке
- product_question: вопрос о характеристиках товара
- purchase_confirmed: покупатель подтверждает покупку ("беру", "оформляю", "давайте", "договорились", "покупаю", "хочу купить", "оплатил")
- complaint_simple: простая жалоба
- dispute: спор, возврат, конфликт
- review_request: просьба об отзыве
- other: другое

Верни только одно слово из списка."""

        try:
            result = await self.call_haiku(
                system="Ты — классификатор намерений. Возвращай только одно слово.",
                user=prompt,
                max_tokens=20,
            )
            intent = result.strip().lower()
            valid_intents = {
                "greeting", "availability", "size_question", "price_negotiation",
                "shipping_question", "product_question", "purchase_confirmed",
                "complaint_simple", "dispute", "review_request", "other",
            }
            return intent if intent in valid_intents else "other"
        except Exception:
            return "other"

    async def _generate_reply(self, msg: dict, intent: str) -> str:
        listing_title = msg.get("listing_title", "наш товар")
        listing_price = msg.get("listing_price", "")
        buyer_name = msg.get("buyer_name", "")

        intent_guides = {
            "greeting": "Поприветствуй, спроси чем помочь, упомяни товар",
            "availability": "Подтверди наличие товара, предложи оформить покупку",
            "size_question": "Скажи что уточнишь размер, попроси написать нужный",
            "shipping_question": "Расскажи про доставку (СДЭК, Почта России), сроки 2-7 дней",
            "product_question": "Ответь на вопрос о товаре, подчеркни качество",
            "purchase_confirmed": "Поблагодари, скажи что оформляешь заказ и свяжешься с деталями доставки",
            "complaint_simple": "Извинись, предложи помощь, будь вежлив",
            "review_request": "Поблагодари, скажи что рад помочь",
            "other": "Ответь вежливо и по существу",
        }

        guide = intent_guides.get(intent, "Ответь вежливо")

        prompt = f"""Напиши ответ покупателю на Avito.
Товар: {listing_title} {f'({listing_price})' if listing_price else ''}
Покупатель: {buyer_name}
Его сообщение: "{msg.get('text', '')}"
Намерение: {intent}
Инструкция: {guide}

Требования:
- Максимум 150 символов
- Дружелюбно, по-деловому
- На русском языке
- Без эмодзи в начале
- Завершай призывом к действию"""

        try:
            reply = await self.call_haiku(
                system="Ты — менеджер по продажам одежды на Avito. Отвечай коротко и по делу.",
                user=prompt,
                max_tokens=200,
            )
            return reply.strip()
        except Exception as exc:
            self._log.error("client_manager.generate_reply_error", error=str(exc))
            return "Здравствуйте! Спасибо за интерес к товару. Готов ответить на ваши вопросы."

    async def _handle_negotiation(self, msg: dict) -> str:
        listing = await self._load_listing_by_avito(msg.get("listing_id"))
        listed_price = 0
        floor_price = 0

        if listing:
            from core.database import Product
            async with get_session() as session:
                prod_result = await session.execute(
                    select(Product).where(Product.id == listing.product_id)
                )
                product = prod_result.scalar_one_or_none()
                if product:
                    listed_price = float(product.price_rub or 0)
                    cost = float(product.cost_rub or 0)
                    min_price = cost / (1 - settings.min_margin_percent / 100)
                    floor_price = max(int(min_price), int(listed_price * (1 - MAX_DISCOUNT_PCT)))

        prompt = f"""Покупатель торгуется за товар на Avito.
Его сообщение: "{msg.get('text', '')}"
Цена в объявлении: {listed_price} руб.
Минимальная цена (не называй): {floor_price} руб.
Максимальная скидка: {MAX_DISCOUNT_PCT * 100:.0f}%

Ответь:
1. Если просят слишком большую скидку — мягко откажи, предложи {floor_price} руб.
2. Если просят разумную скидку — согласись на {floor_price} руб.
3. Будь дружелюбным, не теряй покупателя

Максимум 120 символов. Только ответное сообщение."""

        try:
            reply = await self.call_sonnet(
                system="Ты — опытный продавец одежды. Умеешь торговаться не теряя прибыли.",
                user=prompt,
                max_tokens=200,
            )
            return reply.strip()
        except Exception:
            return "Добрый день! Могу сделать небольшую скидку. Напишите какую цену рассматриваете?"

    # ── Sale notification → n8n → Sheets ──────────────────────

    async def _notify_sale(self, msg: dict) -> None:
        """Стреляем в n8n webhook — он пишет строку в Google Sheets."""
        price_str = msg.get("listing_price", "0").replace("₽", "").replace(" ", "").strip()
        try:
            price = int(price_str)
        except ValueError:
            price = 0

        payload = {
            "date": datetime.now().strftime("%d.%m"),
            "name": msg.get("listing_title", ""),
            "size": "",                        # покупатель должен был написать размер раньше
            "status": "Ожидает отправки",
            "price": price,
            "article": "",                     # добавим в v2 через webhook склада
        }

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(N8N_ORDERS_WEBHOOK, json=payload)
                if resp.status_code == 200:
                    self._log.info("client_manager.sale_notified", payload=payload)
                    await self.report_to_telegram(
                        f"🛍 Новый заказ!\n"
                        f"Товар: {payload['name']}\n"
                        f"Цена: {price} ₽\n"
                        f"Покупатель: {msg.get('buyer_name', '?')}"
                    )
                else:
                    self._log.warning("client_manager.sale_notify_failed",
                                      status=resp.status_code)
        except Exception as exc:
            self._log.error("client_manager.sale_notify_error", error=str(exc))

    # ── Send reply ─────────────────────────────────────────────

    async def _send_reply(self, token: str, chat_id: str, text: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{settings.avito_api_base_url}/messenger/v1/accounts/{settings.avito_user_id}/chats/{chat_id}/messages",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json={"message": {"text": text}, "type": "text"},
                )
                if resp.status_code == 402:
                    self._log.error("client_manager.send_reply_subscription_required")
                    return False
                return resp.status_code in (200, 201)
        except Exception as exc:
            self._log.error("client_manager.send_reply_error", error=str(exc))
            return False

    # ── Follow-up sweep ────────────────────────────────────────

    async def _send_followups(self) -> dict:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FOLLOW_UP_HOURS)
        async with get_session() as session:
            result = await session.execute(
                select(Message).where(
                    Message.direction == "in",
                    Message.status == "replied",
                    Message.responded_at <= cutoff,
                ).limit(20)
            )
            messages = result.scalars().all()

        token = await self._get_avito_token()
        sent = 0
        for msg in messages:
            followup = await self._generate_followup(msg)
            if followup and token:
                success = await self._send_reply(token, msg.buyer_contact, followup)
                if success:
                    async with get_session() as session:
                        await session.execute(
                            update(Message)
                            .where(Message.id == msg.id)
                            .values(status="followup_sent")
                        )
                    sent += 1

        return {"status": "ok", "followups_sent": sent}

    async def _generate_followup(self, msg: Message) -> str:
        try:
            result = await self.call_haiku(
                system="Ты — менеджер по продажам. Пишешь короткий follow-up.",
                user=f"""Покупатель интересовался товаром, но не ответил 24 часа.
Его последнее сообщение: "{msg.content[:100] if msg.content else ''}"
Напиши мягкий follow-up, напомни о товаре, создай лёгкую срочность.
Максимум 100 символов. Только текст сообщения.""",
                max_tokens=150,
            )
            return result.strip()
        except Exception:
            return "Здравствуйте! Товар ещё в наличии. Успейте оформить — остаток ограничен 🙂"

    # ── Review collection ──────────────────────────────────────

    async def _collect_reviews(self) -> dict:
        async with get_session() as session:
            result = await session.execute(
                select(Order).where(Order.status == "done").limit(20)
            )
            orders = result.scalars().all()

        token = await self._get_avito_token()
        if not token:
            return {"status": "error"}

        requested = 0
        for order in orders:
            if not order.buyer_name:
                continue
            try:
                await self.call_haiku(
                    system="Пишешь вежливую просьбу об отзыве.",
                    user=f"Попроси покупателя {order.buyer_name} оставить отзыв на Avito. 80 символов max.",
                    max_tokens=120,
                )
                requested += 1
                async with get_session() as upd_session:
                    await upd_session.execute(
                        update(Order).where(Order.id == order.id).values(status="review_requested")
                    )
            except Exception as exc:
                self._log.warning("client_manager.review_request_error", error=str(exc))

        return {"status": "ok", "review_requests": requested}

    # ── Escalation ─────────────────────────────────────────────

    async def _escalate(self, msg: dict, reason: str) -> None:
        await self.report_to_telegram(
            f"🚨 *Client Manager — Эскалация*\n"
            f"Причина: `{reason}`\n"
            f"Покупатель: {msg.get('buyer_name', '?')}\n"
            f"Товар: {msg.get('listing_title', '?')}\n"
            f"Сообщение: {msg.get('text', '')[:200]}"
        )

    # ── DB helpers ─────────────────────────────────────────────

    async def _save_inbound_message(self, msg: dict) -> int:
        async with get_session() as session:
            m = Message(
                listing_id=msg.get("listing_id"),
                buyer_contact=msg.get("chat_id", ""),
                content=msg.get("text", ""),
                direction="in",
                status="new",
            )
            session.add(m)
            await session.flush()
            return m.id

    async def _save_outbound_message(self, msg: dict, reply: str, parent_id: int) -> None:
        async with get_session() as session:
            m = Message(
                listing_id=msg.get("listing_id"),
                buyer_contact=msg.get("chat_id", ""),
                content=reply,
                direction="out",
                responded_at=datetime.now(timezone.utc),
                status="replied",
            )
            session.add(m)

    async def _update_message_status(self, message_id: int, status: str) -> None:
        async with get_session() as session:
            await session.execute(
                update(Message).where(Message.id == message_id).values(
                    status=status,
                    responded_at=datetime.now(timezone.utc),
                )
            )

    async def _load_listing_by_avito(self, avito_id: Optional[str]) -> Optional[Listing]:
        if not avito_id:
            return None
        async with get_session() as session:
            result = await session.execute(
                select(Listing).where(Listing.avito_id == str(avito_id))
            )
            return result.scalar_one_or_none()
