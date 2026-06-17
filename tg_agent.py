import os
import re
import json
import httpx
import base64
import asyncio
import asyncpg
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import anthropic

TELEGRAM_TOKEN = os.getenv("CLIENT_BOT_TOKEN")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
DATABASE_URL = os.getenv("DATABASE_URL_ASYNCPG", os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://"))
N8N_INVENTORY_URL = "https://tywer1265.app.n8n.cloud/webhook/inventory"
N8N_ORDERS_URL = "https://tywer1265.app.n8n.cloud/webhook/orders/new"
N8N_CLIENTS_URL = "https://tywer1265.app.n8n.cloud/webhook/clients"

MAX_MESSAGES_PER_MINUTE = 10

PURCHASE_KEYWORDS = [
    "оплатил", "оплачено", "перевел", "перевёл", "отправил деньги",
    "оплату сделал", "скинул деньги", "перевео",
]

client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
order_context = {}
spam_counter = {}
followup_tasks = {}
paused_chats = {}  # chat_id → timestamp когда поставлен на паузу
db_pool = None
_inventory_cache: tuple[str, list, float] = ("", [], 0.0)


# ── База данных ────────────────────────────────────────────────

async def init_db():
    global db_pool
    if not DATABASE_URL:
        print("[db] DATABASE_URL не задан — память отключена")
        return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tg_conversations (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    role VARCHAR(16) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tg_conv_chat_id
                ON tg_conversations(chat_id, created_at DESC)
            """)
        print("[db] Подключено, таблица tg_conversations готова")
    except Exception as e:
        print(f"[db] Ошибка подключения: {e}")
        db_pool = None


async def load_history(chat_id: int, limit: int = 30) -> list:
    if not db_pool:
        return []
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT role, content FROM (
                    SELECT role, content, created_at
                    FROM tg_conversations
                    WHERE chat_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                ) sub ORDER BY created_at ASC
            """, chat_id, limit)
            return [{"role": r["role"], "content": r["content"]} for r in rows]
    except Exception as e:
        print(f"[db] load_history error: {e}")
        return []


async def save_message(chat_id: int, role: str, content: str):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO tg_conversations (chat_id, role, content)
                VALUES ($1, $2, $3)
            """, chat_id, role, content)
    except Exception as e:
        print(f"[db] save_message error: {e}")


async def clear_history(chat_id: int):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM tg_conversations WHERE chat_id = $1", chat_id
            )
    except Exception as e:
        print(f"[db] clear_history error: {e}")


# ── Антиспам ───────────────────────────────────────────────────

def is_spam(chat_id: int) -> bool:
    now = datetime.now().timestamp()
    if chat_id not in spam_counter:
        spam_counter[chat_id] = []
    spam_counter[chat_id] = [t for t in spam_counter[chat_id] if now - t < 60]
    spam_counter[chat_id].append(now)
    return len(spam_counter[chat_id]) > MAX_MESSAGES_PER_MINUTE


# ── Утилиты ────────────────────────────────────────────────────

def _is_purchase(text: str) -> bool:
    return any(kw in text.lower() for kw in PURCHASE_KEYWORDS)


def _extract_price(text: str) -> int:
    match = re.search(r"(\d[\d\s]*)\s*[₽р]", text)
    if match:
        return int(match.group(1).replace(" ", ""))
    return 0


def _extract_phone(text: str) -> str:
    """Строго 11 цифр начиная с 7/8, игнорирует 16-значные номера карт."""
    matches = re.findall(r'(?<!\d)(?:\+7|8|7)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}(?!\d)', text)
    if matches:
        return re.sub(r'[\s\-\(\)]', '', matches[0])
    matches2 = re.findall(r'(?<!\d)[78]\d{10}(?!\d)', text.replace(' ', '').replace('-', ''))
    return matches2[0] if matches2 else ""


async def _parse_contacts_async(text: str) -> dict:
    """Async парсинг ФИО/телефон/адрес через Claude."""
    try:
        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                system="""Извлеки из текста данные получателя посылки. Верни ТОЛЬКО JSON без пояснений и без markdown:
{"fio": "...", "phone": "...", "address": "..."}

Правила:
- fio: полное имя (любой регистр, транслит, иностранное имя). Если 2+ слова похожи на имя — это fio
- phone: ровно 11 цифр начинается на 7 или 8. ИГНОРИРУЙ номера карт (16 цифр типа 2200700986188158). Если нет телефона — пустая строка
- address: улица, дом, город. Если нет — пустая строка
- ТОЛЬКО JSON, никакого текста вокруг""",
                messages=[{"role": "user", "content": text}]
            )
        response = await loop.run_in_executor(None, _call)
        raw = response.content[0].text.strip()
        # Убираем markdown если есть
        raw = re.sub(r'```json|```', '', raw).strip()
        result = json.loads(raw)
        return {
            "fio": result.get("fio", ""),
            "phone": result.get("phone", ""),
            "address": result.get("address", "")
        }
    except Exception as e:
        print(f"[contacts_parse] error: {e}")
        return {"fio": "", "phone": _extract_phone(text), "address": ""}


def _update_order_context_sync(chat_id: int, text: str, inventory: list) -> None:
    """Синхронная часть — товар, размер, цена. БЕЗ парсинга контактов."""
    if chat_id not in order_context:
        order_context[chat_id] = {
            "name": "", "size": "", "price": 0, "cost": 0, "article": "",
            "recipient_name": "", "recipient_address": "", "recipient_phone": ""
        }

    text_lower = text.lower()

    # Определяем тип одежды из текста
    ITEM_TYPES = {
        "футболк": ["футболк"],
        "худи": ["худи"],
        "свитшот": ["свитшот"],
        "лонгслив": ["лонгслив"],
        "поло": ["поло"],
        "шоппер": ["шоппер"],
    }
    detected_type = None
    for type_key, keywords in ITEM_TYPES.items():
        if any(kw in text_lower for kw in keywords):
            detected_type = type_key
            break

    # Матч товара с учётом типа одежды
    best_match = None
    best_score = 0
    for item in inventory:
        item_name = str(item.get("name", ""))
        item_lower = item_name.lower()

        # Если тип определён — пропускаем товары другого типа
        if detected_type and detected_type not in item_lower:
            continue

        words = [w for w in item_lower.split() if len(w) > 2]
        if not words:
            continue
        score = sum(1 for w in words if w in text_lower)
        if score > best_score and score >= max(1, len(words) // 2):
            best_score = score
            best_match = item

    # Если с фильтром по типу ничего не нашли — ищем без фильтра
    if not best_match and detected_type:
        for item in inventory:
            item_name = str(item.get("name", ""))
            item_lower = item_name.lower()
            words = [w for w in item_lower.split() if len(w) > 2]
            if not words:
                continue
            score = sum(1 for w in words if w in text_lower)
            if score > best_score and score >= max(1, len(words) // 2):
                best_score = score
                best_match = item

    if best_match:
        order_context[chat_id]["name"] = str(best_match.get("name", ""))
        order_context[chat_id]["article"] = str(best_match.get("article", ""))
        price = best_match.get("price", 0)
        if isinstance(price, str):
            price = _extract_price(price)
        order_context[chat_id]["price"] = price
        cost = best_match.get("cost", 0)
        if isinstance(cost, str):
            cost = int(cost) if str(cost).strip().isdigit() else 0
        order_context[chat_id]["cost"] = cost

    # Размер
    size_match = re.search(r"\b(3XL|XXL|XL|XS|[SML])\b", text, re.IGNORECASE)
    if size_match:
        order_context[chat_id]["size"] = size_match.group(1).upper()

    # Цена из текста
    price = _extract_price(text)
    if price > 0 and price < 50000:
        order_context[chat_id]["price"] = price


async def _update_contacts_if_needed(chat_id: int, text: str) -> None:
    """Async парсинг контактов — только если поля ещё пустые."""
    ctx = order_context.get(chat_id, {})
    already_has_all = (
        ctx.get("recipient_name") and
        ctx.get("recipient_phone") and
        ctx.get("recipient_address")
    )
    if already_has_all:
        return

    # Проверяем что текст похож на данные получателя
    has_digits = bool(re.search(r'\d{7,}', text))
    has_address_hint = any(w in text.lower() for w in [
        "улица", "ул.", "проспект", "пр.", "город", "москва", "санкт", "питер",
        "площадь", "д.", "кв.", "спб", "нск", "екб", "красная", "ленина",
        "пушкина", "мира", "победы", "советская", "кржижановского", "путина",
        "садовая", "лесная", "центральная", "школьная", "молодёжная"
    ])
    word_count = len(text.split())

    if not (has_digits or has_address_hint or word_count >= 3):
        return

    contacts = await _parse_contacts_async(text)
    print(f"[contacts] parsed: {contacts}")

    # Заполняем только пустые поля
    if contacts["fio"] and not ctx.get("recipient_name"):
        order_context[chat_id]["recipient_name"] = contacts["fio"]
    if contacts["phone"] and not ctx.get("recipient_phone"):
        order_context[chat_id]["recipient_phone"] = contacts["phone"]
    if contacts["address"] and not ctx.get("recipient_address"):
        order_context[chat_id]["recipient_address"] = contacts["address"]


async def get_inventory() -> tuple[str, list]:
    global _inventory_cache
    text, items, ts = _inventory_cache
    if items and (datetime.now().timestamp() - ts) < 300:
        return text, items
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            resp = await http.get(N8N_INVENTORY_URL)
            data = resp.json()
            items = data.get("inventory", [])
            if not items:
                return "Склад временно недоступен", []
            lines = [
                f"- {i['name']} ({i['size']}): {i['price']}₽, остаток: {i['stock']} шт"
                for i in items
            ]
            text = "\n".join(lines)
            _inventory_cache = (text, items, datetime.now().timestamp())
            return text, items
    except Exception:
        return text if text else "Склад временно недоступен", items


async def analyze_photo(photo_bytes: bytes, inventory_text: str) -> str:
    image_data = base64.standard_b64encode(photo_bytes).decode("utf-8")
    try:
        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=f"""Ты — менеджер магазина одежды LOCAL Store.
Покупатель прислал фото одежды которую хочет купить.

НАШИ ТОВАРЫ В НАЛИЧИИ:
{inventory_text}

Твоя задача:
1. Определи что на фото (тип одежды, бренд, стиль, цвет)
2. Найди максимально похожий товар из нашего склада
3. Предложи его покупателю коротко и по делу
4. Если похожего нет — честно скажи и предложи альтернативу

Отвечай на русском, 2-3 предложения. Без звёздочек и форматирования. Один смайлик максимум.""",
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/jpeg", "data": image_data}
                        },
                        {"type": "text", "text": "Что это за одежда и есть ли у вас похожее?"}
                    ]
                }]
            )
        response = await loop.run_in_executor(None, _call)
        return response.content[0].text
    except Exception as e:
        print(f"[vision] error: {e}")
        return "Не смог распознать фото. Опишите словами что ищете — помогу найти!"


# ── Followup ────────────────────────────────────────────────────

async def send_followup(bot, chat_id: int) -> None:
    try:
        await asyncio.sleep(2 * 60 * 60)
        if chat_id in followup_tasks:
            await bot.send_message(
                chat_id=chat_id,
                text="Добрый день! Хотел уточнить — всё ли в порядке с оплатой? Если возникли вопросы — напишите, помогу 😊"
            )
            del followup_tasks[chat_id]
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[followup] error: {e}")


def schedule_followup(bot, chat_id: int) -> None:
    cancel_followup(chat_id)
    task = asyncio.create_task(send_followup(bot, chat_id))
    followup_tasks[chat_id] = task
    print(f"[followup] запланирован для {chat_id}")


def cancel_followup(chat_id: int) -> None:
    if chat_id in followup_tasks:
        followup_tasks[chat_id].cancel()
        del followup_tasks[chat_id]
        print(f"[followup] отменён для {chat_id}")


# ── CRM / Уведомления ──────────────────────────────────────────

async def notify_client(user: object, chat_id: int) -> None:
    ctx = order_context.get(chat_id, {})
    payload = {
        "chat_id": chat_id,
        "username": f"@{user.username}" if user.username else "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "fio": ctx.get("recipient_name", ""),
        "phone": ctx.get("recipient_phone", ""),
        "address": ctx.get("recipient_address", ""),
        "city": "",
    }
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(N8N_CLIENTS_URL, json=payload)
    except Exception as e:
        print(f"[crm] notify_client error: {e}")


async def notify_sale(user_name: str, chat_id: int) -> dict:
    ctx = order_context.get(chat_id, {})
    price = ctx.get("price", 0) or 0
    cost = ctx.get("cost", 0) or 0
    profit = price - cost if price and cost else 0
    payload = {
        "date": datetime.now().strftime("%d.%m"),
        "name": ctx.get("name", ""),
        "size": ctx.get("size", ""),
        "status": "Ожидает отправки",
        "price": price,
        "cost": cost,
        "profit": profit,
        "article": ctx.get("article", ""),
        "buyer": user_name,
        "recipient_name": ctx.get("recipient_name", ""),
        "recipient_address": ctx.get("recipient_address", ""),
        "recipient_phone": ctx.get("recipient_phone", ""),
    }
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            await http.post(N8N_ORDERS_URL, json=payload)
    except Exception as e:
        print(f"[sale] webhook error: {e}")
    return payload


def build_system_prompt(inventory_text: str) -> str:
    return f"""Ты — менеджер магазина одежды LOCAL Store. Твоя цель — чтобы каждый покупатель был счастлив и вернулся снова.

ТОВАРЫ В НАЛИЧИИ:
{inventory_text}

О ТОВАРАХ:
Все товары — премиум качества. Есть с брендовыми принтами и базовые без принта — все одинаково высокого качества.
Футболки: 100% хлопок, ткань пенье-компакт, оверсайз крой, принт DTF (держится вечно), плотная.
Худи и свитшоты: 80% хлопок 20% полиэстер, ткань футер, плотные, принт везде одинакового качества.
Лонгсливы: качество как у футболок.
Поло (короткий и длинный рукав): 100% хлопок, ткань пике, премиум качество.
Размеры: от XXS до 3XL.
Можем нанести принт на заказ — от 3 рабочих дней.
Уход: стирка 30-40°С деликатный режим, сушить в расправленном виде, гладить изнаночную сторону, не отбеливать.

ДОСТАВКА:
Отправка в течение 2 дней после оплаты.
Способы: Яндекс доставка, СДЭК, Почта России, Авито доставка, курьер по Москве и МО, самовывоз Москва.

ОПЛАТА:
Перевод на Тинькофф: Артём А., карта 2200700986188158 или по номеру +79776810910.
Либо через Авито при покупке там.

ВОЗВРАТ И ГАРАНТИЯ:
Возврат в течение 14 дней.
Гарантия на товары 12 месяцев.
При проблемах писать сюда или менеджеру @KROSHIDEMANAGER.

СТИЛЬ ОБЩЕНИЯ:
- Всегда обращайся на Вы, уважительно и тепло
- Пиши живо, как настоящий живой человек — не робот
- Максимум ОДИН смайлик на всё сообщение
- Никакого форматирования — никаких звёздочек, решёток, тире в начале строк
- Отвечай коротко — 2-4 предложения максимум
- Никогда не здоровайся повторно если уже поздоровался в этом диалоге
- Используй "подскажите" вместо "расскажите"
- Не называй цену первым — сначала узнай что нужно покупателю
- Покупатель может хотеть несколько товаров — учитывай это
- Никогда не занижай товар — все вещи премиум качества

ПРАВИЛА ПО РАЗМЕРАМ:
- Рекомендуй ОДИН конкретный размер который лучше всего подходит
- Не предлагай два размера сразу
- До 175 см — M, 175-185 см — L, выше 185 см — XL
- Вес выше 90 кг — добавляй один размер вверх

ПРАВИЛА:
1. При торге можешь снизить цену максимум на 10%
2. Если покупатель грубит — вежливо но твёрдо отвечай
3. При эскалации (возврат, конфликт) — напиши ЭСКАЛАЦИЯ: в начале ответа
4. Если товара нет — честно скажи и предложи альтернативу
5. СТРОГО: когда покупатель выбирает товар — сначала уточни способ доставки, затем напиши ТОЛЬКО "Подскажите пожалуйста Ваше полное ФИО для оформления заказа." — адрес и телефон система запросит сама
6. СТРОГО ЗАПРЕЩЕНО писать ПОКУПКА: пока покупатель не написал что уже перевёл деньги (слова: перевёл, оплатил, перевео, скинул)
7. После слова ПОКУПКА: — поблагодари, скажи что отправишь в течение 2 дней и дашь трек-номер"""


# ── Хендлеры ───────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": "",
        "state": "idle"
    }
    await update.message.reply_text(
        "Здравствуйте! Добро пожаловать в LOCAL Store 😊 У нас большой выбор одежды от топовых брендов — Bape, CDG, Y3, Гоша Рубчинский и другие. Что Вас интересует?"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": "",
        "state": "idle"
    }
    await update.message.reply_text("Диалог сброшен. Начинаем заново!")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_name = update.effective_user.first_name or "Покупатель"

    if is_spam(chat_id):
        await update.message.reply_text("Чуть помедленнее, отвечу на все вопросы по очереди 😊")
        return

    cancel_followup(chat_id)

    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        photo_bytes = await file.download_as_bytearray()

        inventory_text, inventory_items = await get_inventory()
        reply = await analyze_photo(bytes(photo_bytes), inventory_text)

        _update_order_context_sync(chat_id, reply, inventory_items)
        await save_message(chat_id, "user", f"{user_name}: [прислал фото]")
        await save_message(chat_id, "assistant", reply)
        await update.message.reply_text(reply)

    except Exception as e:
        print(f"[photo] error: {e}")
        await update.message.reply_text("Не смог обработать фото. Опишите словами что ищете!")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_text = update.message.text
    user_name = update.effective_user.first_name or "Покупатель"

    if is_spam(chat_id):
        await update.message.reply_text("Чуть помедленнее, отвечу на все вопросы по очереди 😊")
        return

    # Если агент на паузе — молчим
    if chat_id in paused_chats:
        print(f"[pause] агент на паузе для {chat_id}, сообщение игнорируется")
        return

    # Покупатель написал — отменяем followup
    cancel_followup(chat_id)

    inventory_text, inventory_items = await get_inventory()

    # Инициализируем контекст если нет
    if chat_id not in order_context:
        order_context[chat_id] = {
            "name": "", "size": "", "price": 0, "cost": 0, "article": "",
            "recipient_name": "", "recipient_address": "", "recipient_phone": "",
            "state": "idle"
        }

    state = order_context[chat_id].get("state", "idle")

    # ── Машина состояний для сбора контактов ──────────────────
    if state == "waiting_fio":
        order_context[chat_id]["recipient_name"] = user_text.strip()
        order_context[chat_id]["state"] = "waiting_address"
        await update.message.reply_text("Подскажите адрес доставки (город, улица, дом, квартира).")
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        await save_message(chat_id, "assistant", "Подскажите адрес доставки.")
        return

    if state == "waiting_address":
        order_context[chat_id]["recipient_address"] = user_text.strip()
        order_context[chat_id]["state"] = "waiting_phone"
        await update.message.reply_text("И последнее — номер телефона для связи.")
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        await save_message(chat_id, "assistant", "И последнее — номер телефона.")
        return

    if state == "waiting_phone":
        order_context[chat_id]["recipient_phone"] = user_text.strip()
        order_context[chat_id]["state"] = "idle"
        ctx = order_context[chat_id]
        price = ctx.get("price", 0)
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        reply = (
            f"Спасибо! Вот реквизиты для оплаты:\n\n"
            f"Перевод на Тинькофф карту: 2200700986188158\n"
            f"Или по номеру: +79776810910\n"
            f"Имя получателя: Артём А.\n\n"
            f"Сумма: {price}₽\n\n"
            f"После перевода напишите мне, и я оформлю отправку."
        )
        await update.message.reply_text(reply)
        await save_message(chat_id, "assistant", reply)
        schedule_followup(context.bot, chat_id)
        return
    # ──────────────────────────────────────────────────────────

    # Синхронно обновляем товар/размер/цену
    _update_order_context_sync(chat_id, user_text, inventory_items)

    # CRM
    await notify_client(update.effective_user, chat_id)

    # Алерт владельцу о новом сообщении (для ручного ответа)
    try:
        owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if owner_token:
            async with httpx.AsyncClient(timeout=3) as http:
                await http.post(
                    f"https://api.telegram.org/bot{owner_token}/sendMessage",
                    json={
                        "chat_id": OWNER_CHAT_ID,
                        "text": f"💬 {user_name} (id:{chat_id}):\n{user_text}\n\n↩️ Reply чтобы ответить",
                        "parse_mode": "HTML"
                    }
                )
    except Exception as e:
        print(f"[owner_alert] error: {e}")

    history = await load_history(chat_id)
    history.append({"role": "user", "content": f"{user_name}: {user_text}"})
    if len(history) > 20:
        history = history[-20:]

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        is_negotiation = any(w in user_text.lower() for w in ["скидк", "дешевле", "уступ", "торг", "снизь"])
        model = "claude-sonnet-4-6" if is_negotiation else "claude-haiku-4-5-20251001"

        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model=model,
                max_tokens=300,
                system=build_system_prompt(inventory_text),
                messages=history
            )
        response = await loop.run_in_executor(None, _call)
        reply = response.content[0].text

        # Обновляем товар/размер из ответа бота
        _update_order_context_sync(chat_id, reply, inventory_items)
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        await save_message(chat_id, "assistant", reply)

        is_sale = _is_purchase(user_text) or reply.startswith("ПОКУПКА:")

        if reply.startswith("ЭСКАЛАЦИЯ:"):
            clean = reply.replace("ЭСКАЛАЦИЯ:", "").strip()
            await update.message.reply_text(clean)
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🚨 ЭСКАЛАЦИЯ от {user_name} (id:{chat_id})!\n"
                     f"Сообщение: {user_text}\n"
                     f"Ответ: {clean}\n\n"
                     f"💬 Сделай Reply на это сообщение чтобы ответить покупателю"
            )
        elif is_sale:
            clean = reply.replace("ПОКУПКА:", "").strip()
            await update.message.reply_text(clean)
            cancel_followup(chat_id)
            order = await notify_sale(user_name, chat_id)
            ctx = order_context.get(chat_id, {})
            status = "✅ Записано в таблицу" if order["name"] else "⚠️ Товар не определён — проверь таблицу"
            alert_text = (
                f"🛍 Новый заказ!\n"
                f"Покупатель: {user_name} (id:{chat_id})\n"
                f"Товар: {order['name'] or '?'}\n"
                f"Размер: {order['size'] or '?'}\n"
                f"Цена: {order['price'] or '?'} руб.\n"
                f"Себестоимость: {order['cost'] or '?'} руб.\n"
                f"Прибыль: {order['profit'] or '?'} руб.\n"
                f"Артикул: {order['article'] or '?'}\n"
                f"ФИО: {ctx.get('recipient_name') or '?'}\n"
                f"Адрес: {ctx.get('recipient_address') or '?'}\n"
                f"Телефон: {ctx.get('recipient_phone') or '?'}\n"
                f"{status}\n\n"
                f"💬 Reply чтобы ответить покупателю"
            )
            # Отправляем в бот владельца через HTTP
            try:
                owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
                async with httpx.AsyncClient(timeout=5) as http:
                    await http.post(
                        f"https://api.telegram.org/bot{owner_token}/sendMessage",
                        json={"chat_id": OWNER_CHAT_ID, "text": alert_text}
                    )
            except Exception as e:
                print(f"[order_alert] error: {e}")
        else:
            await update.message.reply_text(reply)
            # Если бот запросил ФИО — переключаем состояние
            reply_lower = reply.lower()
            if any(w in reply_lower for w in ["фио", "полное имя", "ваше имя", "имя и фамилия", "напишите имя"]):
                order_context[chat_id]["state"] = "waiting_fio"
                order_context[chat_id]["recipient_name"] = ""
                order_context[chat_id]["recipient_address"] = ""
                order_context[chat_id]["recipient_phone"] = ""

    except Exception as e:
        await update.message.reply_text("Секунду, уточню информацию и отвечу!")
        print(f"Error: {e}")


async def handle_owner_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Владелец делает Reply на алерт — пересылаем покупателю и ставим агента на паузу."""
    if not update.message.reply_to_message:
        return

    original_text = update.message.reply_to_message.text or ""
    owner_reply = update.message.text

    match = re.search(r'id:(\d+)', original_text)
    if not match:
        await update.message.reply_text("❌ Не могу найти id покупателя в сообщении")
        return

    buyer_chat_id = int(match.group(1))

    try:
        client_token = os.getenv("CLIENT_BOT_TOKEN")
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(
                f"https://api.telegram.org/bot{client_token}/sendMessage",
                json={"chat_id": buyer_chat_id, "text": owner_reply}
            )
        # Ставим агента на паузу для этого покупателя
        paused_chats[buyer_chat_id] = datetime.now().timestamp()
        await update.message.reply_text(
            f"✅ Отправлено покупателю\n"
            f"⏸ Агент отключён\n\n"
            f"Чтобы включить обратно: /resume {buyer_chat_id}\n"
            f"Или автоматически через 30 минут"
        )
        print(f"[owner_reply] отправлено {buyer_chat_id}, агент на паузе")

        # Автоматическое включение через 30 минут
        async def auto_resume():
            await asyncio.sleep(30 * 60)
            if paused_chats.get(buyer_chat_id):
                del paused_chats[buyer_chat_id]
                try:
                    owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
                    async with httpx.AsyncClient(timeout=5) as h:
                        await h.post(
                            f"https://api.telegram.org/bot{owner_token}/sendMessage",
                            json={
                                "chat_id": OWNER_CHAT_ID,
                                "text": f"▶️ Агент автоматически включён для покупателя id:{buyer_chat_id}"
                            }
                        )
                except Exception:
                    pass

        asyncio.create_task(auto_resume())

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        print(f"[owner_reply] error: {e}")


async def handle_owner_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команды владельца: /resume chat_id."""
    text = update.message.text or ""
    if text.startswith("/resume"):
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("Использование: /resume 123456789")
            return
        try:
            buyer_chat_id = int(parts[1])
            if buyer_chat_id in paused_chats:
                del paused_chats[buyer_chat_id]
            await update.message.reply_text(f"▶️ Агент включён для покупателя {buyer_chat_id}")
        except ValueError:
            await update.message.reply_text("❌ Неверный chat_id")


def main():
    async def run():
        await init_db()

        # Бот покупателей
        client_app = (
            Application.builder()
            .token(TELEGRAM_TOKEN)
            .build()
        )
        client_app.add_handler(CommandHandler("start", start))
        client_app.add_handler(CommandHandler("reset", reset))
        client_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        client_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

        # Бот владельца
        owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
        owner_app = None
        if owner_token and owner_token != TELEGRAM_TOKEN:
            owner_app = (
                Application.builder()
                .token(owner_token)
                .build()
            )
            owner_app.add_handler(
                MessageHandler(filters.TEXT & filters.REPLY, handle_owner_reply)
            )

        await client_app.initialize()
        await client_app.start()
        await client_app.updater.start_polling(drop_pending_updates=True)

        if owner_app:
            owner_app.add_handler(CommandHandler("resume", handle_owner_commands))
            owner_app.add_handler(MessageHandler(filters.TEXT & filters.REPLY, handle_owner_reply))
            await owner_app.initialize()
            await owner_app.start()
            await owner_app.updater.start_polling(drop_pending_updates=True)
            print("Оба бота запущены")
        else:
            print("Бот покупателей запущен")

        try:
            await asyncio.Event().wait()
        finally:
            await client_app.updater.stop()
            await client_app.stop()
            await client_app.shutdown()
            if owner_app:
                await owner_app.updater.stop()
                await owner_app.stop()
                await owner_app.shutdown()

    # Запускаем в новом event loop (для совместимости с threading из main.py)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())


if __name__ == "__main__":
    main()
