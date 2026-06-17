import os
import re
import httpx
import base64
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
    """Извлекает телефон строго 10-11 цифр, игнорирует номера карт (16 цифр)."""
    # Убираем все пробелы и дефисы для анализа, но сохраняем оригинал
    # Ищем телефон: начинается с 7, 8 или +7, строго 10-11 цифр
    matches = re.findall(r'(?<!\d)(?:\+7|8|7)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}(?!\d)', text)
    if matches:
        # Берём первый матч, убираем лишние символы
        phone = re.sub(r'[\s\-\(\)]', '', matches[0])
        return phone
    # Запасной вариант — строго 11 цифр начиная с 7 или 8, не часть более длинного числа
    matches2 = re.findall(r'(?<!\d)[78]\d{10}(?!\d)', text.replace(' ', '').replace('-', ''))
    return matches2[0] if matches2 else ""


def _parse_contacts_from_text(text: str) -> dict:
    """Парсит ФИО, телефон и адрес из произвольного текста покупателя через Claude."""
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system="""Извлеки из текста данные получателя посылки. Верни ТОЛЬКО JSON без пояснений:
{"fio": "...", "phone": "...", "address": "..."}

Правила:
- fio: полное имя (может быть любой регистр, транслит, иностранное имя)
- phone: номер телефона 11 цифр (начинается на 7 или 8). Игнорируй номера карт (16 цифр). Если нет — пустая строка
- address: адрес доставки (город, улица, дом и т.д.). Если нет — пустая строка
- Если данные не найдены — пустая строка для этого поля
- ТОЛЬКО JSON, никакого текста вокруг""",
            messages=[{"role": "user", "content": text}]
        )
        import json
        result = json.loads(response.content[0].text.strip())
        return {
            "fio": result.get("fio", ""),
            "phone": result.get("phone", ""),
            "address": result.get("address", "")
        }
    except Exception as e:
        print(f"[contacts_parse] error: {e}")
        return {"fio": "", "phone": _extract_phone(text), "address": ""}


def _update_order_context(chat_id: int, text: str, inventory: list) -> None:
    if chat_id not in order_context:
        order_context[chat_id] = {
            "name": "", "size": "", "price": 0, "cost": 0, "article": "",
            "recipient_name": "", "recipient_address": "", "recipient_phone": ""
        }

    text_lower = text.lower()

    # Матч товара — ищем точное совпадение названия
    best_match = None
    best_score = 0
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
            cost = int(cost) if cost.strip().isdigit() else 0
        order_context[chat_id]["cost"] = cost

    # Размер
    size_match = re.search(r"\b(3XL|XXL|XL|XS|[SML])\b", text, re.IGNORECASE)
    if size_match:
        order_context[chat_id]["size"] = size_match.group(1).upper()

    # Цена
    price = _extract_price(text)
    if price > 0:
        order_context[chat_id]["price"] = price

    # Контакты — парсим через Claude если текст похож на данные получателя
    # Признаки: есть цифры (телефон) или 2+ слова подряд (ФИО) или адресные слова
    has_digits = bool(re.search(r'\d{7,}', text))
    has_address_hint = any(w in text_lower for w in [
        "улица", "ул.", "проспект", "пр.", "город", "москва", "санкт", "питер",
        "площадь", "д.", "кв.", "спб", "нск", "екб", "красная", "ленина",
        "пушкина", "мира", "победы", "советская"
    ])
    word_count = len(text.split())

    if has_digits or has_address_hint or word_count >= 3:
        contacts = _parse_contacts_from_text(text)
        if contacts["fio"]:
            order_context[chat_id]["recipient_name"] = contacts["fio"]
        if contacts["phone"]:
            order_context[chat_id]["recipient_phone"] = contacts["phone"]
        if contacts["address"]:
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
        response = client.messages.create(
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
        return response.content[0].text
    except Exception as e:
        print(f"[vision] error: {e}")
        return "Не смог распознать фото. Опишите словами что ищете — помогу найти!"


async def notify_client(user: object, chat_id: int) -> None:
    """Отправляет данные клиента в n8n CRM при каждом новом сообщении."""
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
5. СТРОГО: когда покупатель выбирает товар — сначала уточни способ доставки, затем запроси ФИО, адрес и телефон, затем отправь реквизиты
6. СТРОГО ЗАПРЕЩЕНО писать ПОКУПКА: пока покупатель не написал что уже перевёл деньги (слова: перевёл, оплатил, перевео, скинул)
7. После слова ПОКУПКА: — поблагодари, скажи что отправишь в течение 2 дней и дашь трек-номер"""


# ── Хендлеры ───────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": ""
    }
    await update.message.reply_text(
        "Здравствуйте! Добро пожаловать в LOCAL Store 😊 У нас большой выбор одежды от топовых брендов — Bape, CDG, Y3, Гоша Рубчинский и другие. Что Вас интересует?"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": ""
    }
    await update.message.reply_text("Диалог сброшен. Начинаем заново!")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text(
        f"Статистика:\nАктивных контекстов: {len(order_context)}\nПамять: {'PostgreSQL' if db_pool else 'отключена'}"
    )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_name = update.effective_user.first_name or "Покупатель"

    if is_spam(chat_id):
        await update.message.reply_text("Чуть помедленнее, отвечу на все вопросы по очереди 😊")
        return

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        async with httpx.AsyncClient(timeout=15) as http:
            resp = await http.get(file.file_path)
            photo_bytes = resp.content

        inventory_text, inventory_items = await get_inventory()
        reply = await analyze_photo(photo_bytes, inventory_text)

        _update_order_context(chat_id, reply, inventory_items)
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

    inventory_text, inventory_items = await get_inventory()
    _update_order_context(chat_id, user_text, inventory_items)

    # Отправляем данные клиента в CRM (fire and forget)
    await notify_client(update.effective_user, chat_id)

    history = await load_history(chat_id)
    history.append({"role": "user", "content": f"{user_name}: {user_text}"})
    if len(history) > 20:
        history = history[-20:]

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        is_negotiation = any(w in user_text.lower() for w in ["скидк", "дешевле", "уступ", "торг", "снизь"])
        model = "claude-sonnet-4-6" if is_negotiation else "claude-haiku-4-5-20251001"

        response = client.messages.create(
            model=model,
            max_tokens=300,
            system=build_system_prompt(inventory_text),
            messages=history
        )
        reply = response.content[0].text

        _update_order_context(chat_id, reply, inventory_items)
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        await save_message(chat_id, "assistant", reply)

        is_sale = _is_purchase(user_text) or reply.startswith("ПОКУПКА:")

        if reply.startswith("ЭСКАЛАЦИЯ:"):
            clean = reply.replace("ЭСКАЛАЦИЯ:", "").strip()
            await update.message.reply_text(clean)
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🚨 ЭСКАЛАЦИЯ от {user_name}!\nСообщение: {user_text}\nОтвет: {clean}"
            )
        elif is_sale:
            clean = reply.replace("ПОКУПКА:", "").strip()
            await update.message.reply_text(clean)
            order = await notify_sale(user_name, chat_id)
            ctx = order_context.get(chat_id, {})
            status = "✅ Записано в таблицу" if order["name"] else "⚠️ Товар не определён — проверь таблицу"
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🛍 Новый заказ!\n"
                     f"Покупатель: {user_name}\n"
                     f"Товар: {order['name'] or '?'}\n"
                     f"Размер: {order['size'] or '?'}\n"
                     f"Цена: {order['price'] or '?'} руб.\n"
                     f"Себестоимость: {order['cost'] or '?'} руб.\n"
                     f"Прибыль: {order['profit'] or '?'} руб.\n"
                     f"Артикул: {order['article'] or '?'}\n"
                     f"ФИО: {ctx.get('recipient_name') or '?'}\n"
                     f"Адрес: {ctx.get('recipient_address') or '?'}\n"
                     f"Телефон: {ctx.get('recipient_phone') or '?'}\n"
                     f"{status}"
            )
        else:
            await update.message.reply_text(reply)

    except Exception as e:
        await update.message.reply_text("Секунду, уточню информацию и отвечу!")
        print(f"Error: {e}")


def main():
    async def post_init(app):
        await init_db()

    print("Запуск Telegram агента...")
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("Агент запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
