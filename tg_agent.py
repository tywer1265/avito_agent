import os
import re
import json
import httpx
import base64
import asyncio
import asyncpg
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
import anthropic

TELEGRAM_TOKEN = os.getenv("CLIENT_BOT_TOKEN")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
DATABASE_URL = os.getenv("DATABASE_URL_ASYNCPG", os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://"))
N8N_INVENTORY_URL = "https://tywer1265.app.n8n.cloud/webhook/inventory"
N8N_ORDERS_URL = "https://tywer1265.app.n8n.cloud/webhook/orders/new"
N8N_CLIENTS_URL = "https://tywer1265.app.n8n.cloud/webhook/clients"
HQ_CHAT_ID = -1004385799918  # KROSHIDE HQ групповой чат

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
pending_photo_article = {}  # owner_chat_id → article (ждём фото от владельца)
retargeting_tasks = {}  # chat_id → asyncio.Task ретаргетинга через 24ч
upsell_sent = set()  # chat_id где уже отправили upsell


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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS product_photos (
                    id BIGSERIAL PRIMARY KEY,
                    article VARCHAR(64) NOT NULL,
                    file_id TEXT NOT NULL,
                    caption TEXT DEFAULT '',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                ALTER TABLE product_photos ADD COLUMN IF NOT EXISTS caption TEXT DEFAULT ''
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_product_photos_article
                ON product_photos(article)
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wishlist (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_name TEXT DEFAULT '',
                    article VARCHAR(64) NOT NULL,
                    product_name TEXT NOT NULL,
                    size VARCHAR(16) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(chat_id, article, size)
                )
            """)
        print("[db] Подключено, таблицы готовы")
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


# ── Фото товаров ───────────────────────────────────────────────

async def save_product_photo(article: str, file_id: str, caption: str = "") -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO product_photos (article, file_id, caption) VALUES ($1, $2, $3)",
                article.upper(), file_id, caption.lower().strip()
            )
        return True
    except Exception as e:
        print(f"[photos] save error: {e}")
        return False


async def get_product_photos(article: str) -> list:
    """Возвращает список file_id для артикула."""
    if not db_pool:
        return []
    try:
        cyrillic_to_latin = str.maketrans("АВСЕКМНОРТХавсекмнорТх", "ABCEKMHOPTXabcekmhoptx")
        article_norm = article.strip().upper().translate(cyrillic_to_latin)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT file_id FROM product_photos WHERE UPPER(article) = $1 ORDER BY created_at ASC",
                article_norm
            )
        return [r["file_id"] for r in rows]
    except Exception as e:
        print(f"[photos] get error: {e}")
        return []


async def select_photo_by_vision(file_ids: list, user_request: str, product_name: str) -> list:
    """Если клиент просит конкретное фото — Haiku смотрит все и выбирает нужное."""
    # Если запрос общий — возвращаем все
    SPECIFIC_KEYWORDS = ["принт", "лого", "бирк", "детал", "крупно", "ближе",
                         "капюшон", "карман", "рукав", "спин", "перед", "этикет"]
    if not any(kw in user_request.lower() for kw in SPECIFIC_KEYWORDS):
        return file_ids  # общий запрос — все фото

    if len(file_ids) <= 1:
        return file_ids  # одно фото — выбирать не из чего

    try:
        # Скачиваем все фото через клиентский бот
        client_token = os.getenv("CLIENT_BOT_TOKEN")
        content = [{"type": "text", "text": f"Покупатель просит: '{user_request}'\nТовар: {product_name}\n\nПосмотри на фото ниже и выбери ТОЛЬКО те которые соответствуют запросу покупателя. Верни ТОЛЬКО номера фото через запятую (1, 2, 3...). Если подходят все — верни 'все'."}]

        for i, file_id in enumerate(file_ids[:10], 1):
            async with httpx.AsyncClient(timeout=15) as http:
                resp = await http.get(
                    f"https://api.telegram.org/bot{client_token}/getFile",
                    params={"file_id": file_id}
                )
                file_data = resp.json()
                if not file_data.get("ok"):
                    continue
                file_path = file_data["result"]["file_path"]
                img_resp = await http.get(
                    f"https://api.telegram.org/file/bot{client_token}/{file_path}"
                )
                img_b64 = base64.standard_b64encode(img_resp.content).decode()
                content.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
                })
                content.append({"type": "text", "text": f"Фото {i}"})

        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=50,
                messages=[{"role": "user", "content": content}]
            )
        response = await loop.run_in_executor(None, _call)
        answer = response.content[0].text.strip().lower()

        if "все" in answer or "all" in answer:
            return file_ids

        # Парсим номера
        numbers = re.findall(r'\d+', answer)
        selected = []
        for n in numbers:
            idx = int(n) - 1
            if 0 <= idx < len(file_ids):
                selected.append(file_ids[idx])

        return selected if selected else file_ids

    except Exception as e:
        print(f"[vision_select] error: {e}")
        return file_ids  # при ошибке возвращаем все


async def delete_product_photos(article: str) -> int:
    if not db_pool:
        return 0
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM product_photos WHERE article = $1",
                article.upper()
            )
        return int(result.split()[-1])
    except Exception as e:
        print(f"[photos] delete error: {e}")
        return 0



async def find_article_by_name(name_part: str, inventory_items: list) -> str:
    """Ищет артикул по части названия товара."""
    name_lower = name_part.lower()
    for item in inventory_items:
        if name_lower in str(item.get("name", "")).lower():
            return str(item.get("article", ""))
    return ""


# ── Вишлист ────────────────────────────────────────────────────

async def wishlist_add(chat_id: int, user_name: str, article: str, product_name: str, size: str) -> bool:
    if not db_pool:
        return False
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO wishlist (chat_id, user_name, article, product_name, size)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (chat_id, article, size) DO NOTHING
            """, chat_id, user_name, article.upper(), product_name, size.upper())
        return True
    except Exception as e:
        print(f"[wishlist] add error: {e}")
        return False


async def wishlist_notify(bot, article: str, size: str) -> int:
    """Уведомляем всех кто ждёт этот товар+размер."""
    if not db_pool:
        return 0
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT chat_id, product_name FROM wishlist
                WHERE UPPER(article) = $1 AND UPPER(size) = $2
            """, article.upper(), size.upper())

        count = 0
        for row in rows:
            try:
                await bot.send_message(
                    chat_id=row["chat_id"],
                    text=f"Привет! Вы ждали {row['product_name']} размер {size} — он снова появился в наличии 😊 Хотите оформить заказ?"
                )
                count += 1
            except Exception as e:
                print(f"[wishlist] notify error for {row['chat_id']}: {e}")

        # Удаляем уведомлённых
        if rows:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM wishlist
                    WHERE UPPER(article) = $1 AND UPPER(size) = $2
                """, article.upper(), size.upper())

        return count
    except Exception as e:
        print(f"[wishlist] notify error: {e}")
        return 0


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
                f"- {i['name']} ({i['size']}): {i['price']}₽, остаток: {i['stock']} шт" +
                (" ⚠️ ПОСЛЕДНИЙ" if int(i.get('stock', 99)) <= 2 else "")
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
                system=f"""Ты — менеджер магазина одежды KROSHIDE.
Покупатель прислал фото одежды которую хочет купить.

НАШИ ТОВАРЫ В НАЛИЧИИ:
{inventory_text}

Твоя задача:
1. Определи что на фото (тип одежды, бренд, стиль, цвет)
2. Найди максимально похожий товар из нашего склада
3. Предложи его покупателю коротко и по делу
4. Если похожего нет — честно скажи и предложи альтернативу

Отвечай на русском, 2-3 предложения. Без звёздочек и форматирования. Один смайлик максимум.""",
                messages=[{"role": "user", "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/jpeg", "data": image_data}
                        },
                        {"type": "text", "text": "Что это за одежда и есть ли у вас похожее?"}
                    ]}]
            )
        response = await loop.run_in_executor(None, _call)
        return response.content[0].text
    except Exception as e:
        print(f"[vision] error: {e}")
        return "Не смог распознать фото. Опишите словами что ищете — помогу найти!"


async def transcribe_voice(file_bytes: bytes) -> str:
    """Транскрибируем войс через Haiku — описываем что сказал клиент."""
    audio_b64 = base64.standard_b64encode(file_bytes).decode()
    try:
        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                system="""Тебе присылают голосовое сообщение в base64. Это аудио от покупателя магазина одежды.
Твоя задача — распознать и дословно написать что сказал человек на русском языке.
Верни ТОЛЬКО текст того что сказано, без пояснений и комментариев.""",
                messages=[{"role": "user", "content": [
                    {"type": "text", "text": f"Расшифруй голосовое сообщение. Аудио в base64 (ogg/opus): {audio_b64[:100]}..."}
                ]}]
            )
        response = await loop.run_in_executor(None, _call)
        return response.content[0].text.strip()
    except Exception as e:
        print(f"[voice] transcribe error: {e}")
        return ""


def _detect_comparison(text: str) -> bool:
    """Определяем что клиент хочет сравнить товары."""
    triggers = [
        "чем лучше", "чем хуже", "в чём разница", "что лучше", "сравни",
        "отличие", "отличается", "разница между", "или лучше", "что выбрать",
        "какой лучше", "какая лучше"
    ]
    return any(t in text.lower() for t in triggers)


def _detect_stylist(text: str) -> bool:
    """Определяем что клиент хочет подобрать образ."""
    triggers = [
        "подбери образ", "собери образ", "что надеть", "с чем носить",
        "стилист", "подбери комплект", "собери комплект", "полный образ",
        "что сочетается", "лук", "outfit", "что подойдёт к"
    ]
    return any(t in text.lower() for t in triggers)


async def ai_compare(text: str, inventory_text: str) -> str:
    """AI сравнивает два товара по запросу клиента."""
    try:
        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=f"""Ты — эксперт по уличной моде и менеджер магазина KROSHIDE.

ТОВАРЫ В НАЛИЧИИ:
{inventory_text}

Клиент хочет сравнить товары или выбрать между ними.
Объясни разницу честно и по делу — материал, стиль, для кого подходит, цена.
Дай конкретную рекомендацию в конце.
Отвечай 3-5 предложений. Без звёздочек и форматирования. Один смайлик максимум.""",
                messages=[{"role": "user", "content": text}]
            )
        response = await loop.run_in_executor(None, _call)
        return response.content[0].text
    except Exception as e:
        print(f"[compare] error: {e}")
        return ""


async def ai_stylist(text: str, inventory_text: str) -> str:
    """AI стилист подбирает комплект из склада."""
    try:
        loop = asyncio.get_event_loop()
        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                system=f"""Ты — персональный стилист и менеджер магазина KROSHIDE.

ТОВАРЫ В НАЛИЧИИ (только из этого списка подбирай):
{inventory_text}

Клиент описывает желаемый образ или спрашивает что носить.
Подбери конкретный комплект из наших товаров — 2-3 вещи которые сочетаются.
Объясни почему именно эти вещи подходят друг к другу и к описанному образу.
Назови конкретные товары и цены.
Отвечай живо и по делу, 4-6 предложений. Без звёздочек. Один смайлик максимум.""",
                messages=[{"role": "user", "content": text}]
            )
        response = await loop.run_in_executor(None, _call)
        return response.content[0].text
    except Exception as e:
        print(f"[stylist] error: {e}")
        return ""


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


# ── Ретаргетинг через 24ч ──────────────────────────────────────

async def send_retargeting(bot, chat_id: int, product_name: str, size: str) -> None:
    try:
        await asyncio.sleep(24 * 60 * 60)
        if chat_id in retargeting_tasks:
            item_text = f" ({product_name}, размер {size})" if product_name else ""
            msg = f"Кстати, вы смотрели у нас товар{item_text} — он ещё в наличии 😊 Если остались вопросы или хотите оформить — напишите, помогу!"
            await bot.send_message(chat_id=chat_id, text=msg)
            del retargeting_tasks[chat_id]
            print(f"[retargeting] отправлено {chat_id}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[retargeting] error: {e}")


def schedule_retargeting(bot, chat_id: int, product_name: str = "", size: str = "") -> None:
    cancel_retargeting(chat_id)
    task = asyncio.create_task(send_retargeting(bot, chat_id, product_name, size))
    retargeting_tasks[chat_id] = task
    print(f"[retargeting] запланирован для {chat_id} ({product_name})")


def cancel_retargeting(chat_id: int) -> None:
    if chat_id in retargeting_tasks:
        retargeting_tasks[chat_id].cancel()
        del retargeting_tasks[chat_id]


# ── Дефицит — последний размер ─────────────────────────────────

def _check_deficit(inventory_items: list, article: str, size: str) -> bool:
    """Проверяем остаток товара — если ≤2 шт возвращаем True."""
    for item in inventory_items:
        if str(item.get("article", "")).upper() == article.upper():
            stock = item.get("stock", 99)
            try:
                return int(stock) <= 2
            except Exception:
                return False
    return False


# ── Upsell ─────────────────────────────────────────────────────

async def send_upsell(bot, chat_id: int, current_article: str, inventory_items: list) -> None:
    """Через 30 сек после покупки предлагаем второй товар."""
    try:
        await asyncio.sleep(30)
        if chat_id in upsell_sent:
            return
        # Ищем товар другого типа из той же ценовой категории
        ctx = order_context.get(chat_id, {})
        current_price = ctx.get("price", 0)
        current_name = ctx.get("name", "").lower()

        candidates = []
        for item in inventory_items:
            if str(item.get("article", "")).upper() == current_article.upper():
                continue
            item_name = str(item.get("name", "")).lower()
            # Не предлагаем тот же тип
            if any(t in current_name and t in item_name for t in ["худи", "футболк", "лонгслив", "свитшот", "поло"]):
                continue
            item_price = item.get("price", 0)
            try:
                item_price = int(item_price)
            except Exception:
                continue
            if item_price > 0 and int(item.get("stock", 0)) > 0:
                candidates.append(item)

        if not candidates:
            return

        # Берём первый подходящий
        pick = candidates[0]
        upsell_sent.add(chat_id)
        msg = f"Кстати, к вашему заказу отлично подойдёт {pick['name']} — {pick['price']}₽. Хотите покажу фото? 😊"
        await bot.send_message(chat_id=chat_id, text=msg)
        print(f"[upsell] отправлено {chat_id}: {pick['name']}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[upsell] error: {e}")


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
    return f"""Ты — менеджер магазина одежды KROSHIDE. Твоя цель — чтобы каждый покупатель был счастлив и вернулся снова.

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
Отправка в течение 3 дней после оплаты.
Способы доставки:
- Яндекс доставка
- СДЭК
- Почта России
- Авито доставка
- Курьер по Москве и МО — стоимость от 500 рублей. Если покупатель хочет курьера — попроси адрес и скажи что уточнишь точную стоимость
- Самовывоз: Москва, ул. Кржижановского 23к2. Это пункт выдачи, не шоурум. Все заказы выдаются по заявкам. Возможна оплата при получении.

ОПЛАТА:
Перевод на Тинькофф: Артём А., карта 2200700986188158 или по номеру +79776810910.
При самовывозе возможна оплата при получении.
Либо через Авито при покупке там.

ВОЗВРАТ И ГАРАНТИЯ:
Возврат в течение 14 дней.
Гарантия на товары 12 месяцев.
При проблемах писать сюда или менеджеру @KROSHIDEMANAGER.

СТИЛЬ ОБЩЕНИЯ:
- Твоё имя — Тёма, ты живой менеджер магазина KROSHIDE
- Если спрашивают кто ты — честно говори что ты Тёма, менеджер магазина
- Не говори что ты бот или ИИ, но и не отрицай активно — просто веди себя как человек
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
7. После слова ПОКУПКА: — поблагодари, скажи что отправишь в течение 2 дней и дашь трек-номер
8. СТРОГО: если покупатель просит фото — отвечай ТОЛЬКО "Сейчас пришлю фото!" и больше ничего. Система сама отправит фото автоматически. НИКОГДА не говори что не можешь отправить фото.
9. Если у товара пометка ⚠️ ПОСЛЕДНИЙ — обязательно упомяни это: "последний размер", "остался один", "торопитесь". Создай ощущение срочности."""


# ── Хендлеры ───────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": "",
        "delivery": "", "state": "idle"
    }
    await update.message.reply_text(
        "Добро пожаловать в KROSHIDE 😊 У нас большой выбор одежды от топовых брендов — Bape, CDG, Y3, Гоша Рубчинский и другие. Что Вас интересует?"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_history(chat_id)
    order_context[chat_id] = {
        "name": "", "size": "", "price": 0, "cost": 0, "article": "",
        "recipient_name": "", "recipient_address": "", "recipient_phone": "",
        "delivery": "", "state": "idle"
    }
    await update.message.reply_text("Диалог сброшен. Начинаем заново!")


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки подтверждения заказа."""
    query = update.callback_query
    await query.answer()

    chat_id = query.from_user.id
    if not query.data.startswith("confirm_"):
        return

    ctx = order_context.get(chat_id, {})
    price = ctx.get("price", 0)
    delivery = ctx.get("delivery", "не указан")

    order_context[chat_id]["state"] = "idle"

    # Реквизиты для оплаты
    if delivery == "Самовывоз":
        reply = (
            f"Отлично, заказ подтверждён! 😊\n\n"
            f"Адрес самовывоза: Москва, ул. Кржижановского 23к2\n"
            f"Сумма: {price}₽\n\n"
            f"Оплата при получении. Напишите когда будете готовы забрать — согласуем время."
        )
    else:
        reply = (
            f"Отлично, заказ подтверждён! 😊\n\n"
            f"Перевод на Тинькофф карту: 2200700986188158\n"
            f"Или по номеру: +79776810910\n"
            f"Имя получателя: Артём А.\n\n"
            f"Сумма: {price}₽\n\n"
            f"После перевода напишите мне — сразу оформлю отправку."
        )

    await query.edit_message_text(reply)
    await save_message(chat_id, "assistant", reply)
    schedule_followup(context.bot, chat_id)


async def handle_purchase_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель написал что перевёл — фиксируем заказ."""
    pass  # логика уже в handle_message через PURCHASE_KEYWORDS


HQ_TOPIC_TEMA = 10
HQ_TOPIC_TIMKA = 13
HQ_TOPIC_ZAKAZY = 16
HQ_TOPIC_ZADACHI = 2


async def get_hq_context() -> dict:
    """Собираем весь контекст для Тёмы в HQ — клиенты, диалоги, ретаргетинг, вишлист."""
    ctx = {
        "new_clients_today": 0,
        "retargeting_active": 0,
        "wishlist": [],
        "open_dialogs": [],
        "paused_clients": list(paused_chats.keys()),
        "retargeting_clients": [],
    }

    if not db_pool:
        return ctx

    try:
        async with db_pool.acquire() as conn:
            # Новые клиенты сегодня
            today = datetime.now().date()
            rows = await conn.fetch(
                "SELECT COUNT(DISTINCT chat_id) as cnt FROM tg_conversations WHERE DATE(created_at) = $1",
                today
            )
            ctx["new_clients_today"] = rows[0]["cnt"] if rows else 0

            # Незакрытые диалоги — клиенты которые писали но не купили
            rows = await conn.fetch("""
                SELECT DISTINCT chat_id, MAX(created_at) as last_msg
                FROM tg_conversations
                WHERE role = 'user' AND DATE(created_at) = $1
                GROUP BY chat_id
                ORDER BY last_msg DESC
                LIMIT 20
            """, today)
            ctx["open_dialogs"] = [{"chat_id": r["chat_id"], "last_msg": str(r["last_msg"])} for r in rows]

            # Вишлист
            rows = await conn.fetch("""
                SELECT user_name, product_name, size, chat_id, created_at
                FROM wishlist ORDER BY created_at DESC
            """)
            ctx["wishlist"] = [{"name": r["user_name"], "product": r["product_name"], "size": r["size"], "chat_id": r["chat_id"]} for r in rows]

    except Exception as e:
        print(f"[hq_ctx] error: {e}")

    # Ретаргетинг
    ctx["retargeting_active"] = len(retargeting_tasks)
    ctx["retargeting_clients"] = list(retargeting_tasks.keys())

    return ctx


async def handle_hq_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик сообщений в KROSHIDE Online Office."""
    if update.effective_chat.id != HQ_CHAT_ID:
        return

    thread_id = update.message.message_thread_id
    text = (update.message.text or "").strip()

    if not text:
        return

    print(f"[hq] thread_id={thread_id} text={text[:50]}")

    # ── Тема ТЁМА ─────────────────────────────────────────────
    if thread_id == HQ_TOPIC_TEMA:
        await context.bot.send_chat_action(chat_id=HQ_CHAT_ID, action="typing", message_thread_id=thread_id)

        inventory_text, inventory_items = await get_inventory()
        hq_ctx = await get_hq_context()

        # Команды без AI
        text_lower = text.lower()

        # Пауза клиента
        if "пауза" in text_lower or "поставь на паузу" in text_lower:
            parts = text.split()
            for p in parts:
                if p.isdigit():
                    paused_chats[int(p)] = asyncio.get_event_loop().time()
                    await context.bot.send_message(chat_id=HQ_CHAT_ID, text=f"⏸ Клиент {p} поставлен на паузу", message_thread_id=thread_id)
                    return
            await context.bot.send_message(chat_id=HQ_CHAT_ID, text="Укажи chat_id клиента. Пример: пауза 123456789", message_thread_id=thread_id)
            return

        # Снять с паузы
        if "снять" in text_lower or "resume" in text_lower:
            parts = text.split()
            for p in parts:
                if p.isdigit():
                    cid = int(p)
                    if cid in paused_chats:
                        del paused_chats[cid]
                    await context.bot.send_message(chat_id=HQ_CHAT_ID, text=f"▶️ Клиент {p} снят с паузы", message_thread_id=thread_id)
                    return

        # Запустить ретаргетинг вручную
        if "ретаргетинг" in text_lower and ("запусти" in text_lower or "отправь" in text_lower):
            parts = text.split()
            for p in parts:
                if p.isdigit():
                    cid = int(p)
                    ctx_data = order_context.get(cid, {})
                    schedule_retargeting(context.bot, cid, ctx_data.get("name", ""), ctx_data.get("size", ""))
                    await context.bot.send_message(chat_id=HQ_CHAT_ID, text=f"🎯 Ретаргетинг запущен для клиента {p}", message_thread_id=thread_id)
                    return
            # Запустить всем кто в ретаргетинге
            await context.bot.send_message(chat_id=HQ_CHAT_ID, text=f"🎯 Активных ретаргетингов: {hq_ctx['retargeting_active']}\nChat ID: {hq_ctx['retargeting_clients']}", message_thread_id=thread_id)
            return

        # AI отвечает на всё остальное
        wishlist_text = "\n".join([f"- {w['name']} ждёт {w['product']} размер {w['size']} (chat_id: {w['chat_id']})" for w in hq_ctx["wishlist"]]) or "Вишлист пуст"
        dialogs_text = "\n".join([f"- chat_id: {d['chat_id']}, последнее сообщение: {d['last_msg']}" for d in hq_ctx["open_dialogs"]]) or "Нет диалогов сегодня"

        try:
            loop = asyncio.get_event_loop()
            def _call_tema():
                return client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=700,
                    system=f"""Ты — Тёма, менеджер по продажам KROSHIDE. Рабочий чат с владельцем Артёмом.
Отвечай коротко, чётко, с цифрами. Без воды.

СКЛАД:
{inventory_text}

ДАННЫЕ ПО КЛИЕНТАМ:
- Новых клиентов сегодня: {hq_ctx['new_clients_today']}
- Активных ретаргетингов: {hq_ctx['retargeting_active']} чел
- На паузе: {len(hq_ctx['paused_clients'])} чел

ВИШЛИСТ (ждут товар):
{wishlist_text}

ДИАЛОГИ СЕГОДНЯ:
{dialogs_text}

КОМАНДЫ которые ты умеешь:
- "пауза [chat_id]" — поставить клиента на паузу
- "снять [chat_id]" — снять с паузы
- "ретаргетинг [chat_id]" — запустить вручную
- Любой вопрос по складу, клиентам, продажам""",
                    messages=[{"role": "user", "content": text}]
                )
            response = await loop.run_in_executor(None, _call_tema)
            reply = response.content[0].text
            await context.bot.send_message(
                chat_id=HQ_CHAT_ID,
                text=f"🤖 {reply}",
                message_thread_id=thread_id
            )
        except Exception as e:
            print(f"[hq] tema error: {e}")
            await context.bot.send_message(chat_id=HQ_CHAT_ID, text="❌ Ошибка, попробуй ещё раз", message_thread_id=thread_id)

    # ── Тема ЗАКАЗЫ ───────────────────────────────────────────
    elif thread_id == HQ_TOPIC_ZAKAZY:
        await context.bot.send_chat_action(chat_id=HQ_CHAT_ID, action="typing", message_thread_id=thread_id)
        inventory_text, _ = await get_inventory()
        try:
            loop = asyncio.get_event_loop()
            def _call_zakazy():
                return client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=400,
                    system=f"""Ты — Тёма, менеджер KROSHIDE. Отвечаешь на вопросы по заказам.
СКЛАД: {inventory_text}
Отвечай коротко и по делу.""",
                    messages=[{"role": "user", "content": text}]
                )
            response = await loop.run_in_executor(None, _call_zakazy)
            await context.bot.send_message(
                chat_id=HQ_CHAT_ID,
                text=f"📦 {response.content[0].text}",
                message_thread_id=thread_id
            )
        except Exception as e:
            print(f"[hq] zakazy error: {e}")


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Клиент прислал голосовое — просим написать текстом (пока нет OpenAI)."""
    await update.message.reply_text(
        "Не смог обработать голосовое. Напишите текстом — отвечу быстро! 😊"
    )


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

    # Если агент на паузе — пересылаем сообщение владельцу и молчим
    if chat_id in paused_chats:
        try:
            owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
            async with httpx.AsyncClient(timeout=3) as http:
                await http.post(
                    f"https://api.telegram.org/bot{owner_token}/sendMessage",
                    json={
                        "chat_id": OWNER_CHAT_ID,
                        "text": f"💬 {user_name} (id:{chat_id}):\n{user_text}\n\n↩️ Reply чтобы ответить | /resume {chat_id} чтобы включить агента"
                    }
                )
        except Exception as e:
            print(f"[paused_forward] error: {e}")
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
        order_context[chat_id]["state"] = "waiting_confirm"
        ctx = order_context[chat_id]
        price = ctx.get("price", 0)
        delivery = ctx.get("delivery", "не указан")
        await save_message(chat_id, "user", f"{user_name}: {user_text}")

        # Показываем сводку заказа с кнопкой подтверждения
        summary = (
            f"Проверьте Ваш заказ:\n\n"
            f"Товар: {ctx.get('name') or '?'}\n"
            f"Размер: {ctx.get('size') or '?'}\n"
            f"Сумма: {price}₽\n"
            f"Доставка: {delivery}\n"
            f"ФИО: {ctx.get('recipient_name') or '?'}\n"
            f"Адрес: {ctx.get('recipient_address') or '?'}\n"
            f"Телефон: {ctx.get('recipient_phone') or '?'}\n\n"
            f"Всё верно?"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить заказ", callback_data=f"confirm_{chat_id}")]
        ])
        await update.message.reply_text(summary, reply_markup=keyboard)
        await save_message(chat_id, "assistant", summary)
        return

    if state == "waiting_confirm":
        # Покупатель написал текст вместо кнопки — напоминаем
        await update.message.reply_text("Пожалуйста, нажмите кнопку «Подтвердить заказ» выше 😊")
        return
    # ──────────────────────────────────────────────────────────

    # Синхронно обновляем товар/размер/цену
    _update_order_context_sync(chat_id, user_text, inventory_items)

    # ── Отправка фото по запросу клиента ──────────────────────
    PHOTO_TRIGGERS = ["фото", "фотк", "покажи", "покажите", "фоточк", "посмотреть", "как выглядит", "есть фото", "скинь", "пришли фото"]
    if any(t in user_text.lower() for t in PHOTO_TRIGGERS):

        # Берём артикул из контекста
        article = order_context.get(chat_id, {}).get("article", "")
        product_name = order_context.get(chat_id, {}).get("name", "")

        # Если нет — ищем по тексту с транслитерацией
        if not article:
            translit = {
                "у3": "y3", "у2": "y2", "у1": "y1",
                "бейп": "bape", "бэйп": "bape",
                "сдг": "cdg", "гоша": "gosha", "рубчинский": "rubchinskiy",
                "мастермайнд": "mastermind",
            }
            text_norm = user_text.lower()
            for ru, en in translit.items():
                text_norm = text_norm.replace(ru, en)

            best_article = ""
            best_score = 0
            best_name = ""
            for item in inventory_items:
                item_name = str(item.get("name", "")).lower()
                words = [w for w in item_name.split() if len(w) > 1]
                score = sum(1 for w in words if w in text_norm or w in user_text.lower())
                if score > best_score:
                    best_score = score
                    best_article = str(item.get("article", ""))
                    best_name = str(item.get("name", ""))

            if best_score >= 1 and best_article:
                article = best_article
                product_name = best_name
                order_context[chat_id]["article"] = article
                order_context[chat_id]["name"] = product_name

        if article:
            all_photos = await get_product_photos(article)
            print(f"[photo_send] article={article} total={len(all_photos)}")
            if all_photos:
                try:
                    # Vision выбирает нужные фото если запрос конкретный
                    selected = await select_photo_by_vision(all_photos, user_text, product_name)
                    from telegram import InputMediaPhoto
                    await context.bot.send_media_group(
                        chat_id=chat_id,
                        media=[InputMediaPhoto(media=fid) for fid in selected[:10]]
                    )
                    await save_message(chat_id, "user", f"{user_name}: {user_text}")
                    await save_message(chat_id, "assistant", f"[отправлено {len(selected)} фото для {article}]")
                    return
                except Exception as e:
                    print(f"[photo_send] error: {e}")
        else:
            print(f"[photo_send] артикул не найден для: {user_text}")
    # ──────────────────────────────────────────────────────────

    # Сохраняем способ доставки если упоминается
    delivery_map = {
        "яндекс": "Яндекс доставка",
        "сдэк": "СДЭК",
        "почт": "Почта России",
        "авито": "Авито доставка",
        "курьер": "Курьер",
        "самовывоз": "Самовывоз",
        "самовыво": "Самовывоз",
    }
    for key, val in delivery_map.items():
        if key in user_text.lower():
            order_context[chat_id]["delivery"] = val
            break

    # Ранняя эскалация — до ответа Claude
    ESCALATION_TRIGGERS = [
        "хочу возврат", "сделать возврат", "вернуть товар", "не подошел размер",
        "не подошёл размер", "не тот размер", "бракованный", "брак", "не то прислали",
        "живого человека", "реального человека", "позови человека", "позовите человека",
        "менеджера", "руководителя", "верните деньги", "возврат денег"
    ]
    if any(trigger in user_text.lower() for trigger in ESCALATION_TRIGGERS):
        escalation_reply = "Понимаю вас, сейчас передам вопрос менеджеру — он свяжется с вами в ближайшее время 😊"
        await update.message.reply_text(escalation_reply)
        await save_message(chat_id, "user", f"{user_name}: {user_text}")
        await save_message(chat_id, "assistant", escalation_reply)
        try:
            owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
            async with httpx.AsyncClient(timeout=5) as http:
                await http.post(
                    f"https://api.telegram.org/bot{owner_token}/sendMessage",
                    json={
                        "chat_id": OWNER_CHAT_ID,
                        "text": f"🚨 ЭСКАЛАЦИЯ от {user_name} (id:{chat_id})!\n"
                                f"Сообщение: {user_text}\n\n"
                                f"💬 Reply чтобы ответить покупателю"
                    }
                )
        except Exception as e:
            print(f"[escalation] error: {e}")
        return

    # CRM
    await notify_client(update.effective_user, chat_id)

    # Запускаем ретаргетинг — если клиент не купит через 24ч, напомним
    ctx_now = order_context.get(chat_id, {})
    if ctx_now.get("name") and ctx_now.get("state") not in ["waiting_fio", "waiting_address", "waiting_phone", "waiting_confirm"]:
        schedule_retargeting(
            context.bot, chat_id,
            ctx_now.get("name", ""),
            ctx_now.get("size", "")
        )

    # ── AI Сравнение товаров ───────────────────────────────────
    if _detect_comparison(user_text):
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        reply = await ai_compare(user_text, inventory_text)
        if reply:
            await update.message.reply_text(reply)
            await save_message(chat_id, "user", f"{user_name}: {user_text}")
            await save_message(chat_id, "assistant", reply)
            return
    # ──────────────────────────────────────────────────────────

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
            try:
                owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
                async with httpx.AsyncClient(timeout=5) as http:
                    await http.post(
                        f"https://api.telegram.org/bot{owner_token}/sendMessage",
                        json={
                            "chat_id": OWNER_CHAT_ID,
                            "text": f"🚨 ЭСКАЛАЦИЯ от {user_name} (id:{chat_id})!\n"
                                    f"Сообщение: {user_text}\n"
                                    f"Ответ: {clean}\n\n"
                                    f"💬 Reply чтобы ответить покупателю"
                        }
                    )
            except Exception as e:
                print(f"[escalation_alert] error: {e}")
        elif is_sale:
            clean = reply.replace("ПОКУПКА:", "").strip()
            after_payment_msg = "Спасибо за заказ! Передал его на сборку, отправка будет в течение 3х дней, ожидайте. Трек номер пришлю чуть позже! ♥️"
            await update.message.reply_text(after_payment_msg)
            cancel_followup(chat_id)
            cancel_retargeting(chat_id)  # отменяем ретаргетинг — купил
            order = await notify_sale(user_name, chat_id)
            # Запускаем upsell через 30 сек
            ctx = order_context.get(chat_id, {})
            article = ctx.get("article", "")
            if article and chat_id not in upsell_sent:
                asyncio.create_task(send_upsell(context.bot, chat_id, article, inventory_items))
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

            # Вишлист — если агент говорит что нет размера
            NO_SIZE_TRIGGERS = ["нет в наличии", "нет такого размера", "такого размера нет",
                                "закончился", "нет размера", "к сожалению нет", "не осталось"]
            if any(t in reply_lower for t in NO_SIZE_TRIGGERS):
                ctx = order_context.get(chat_id, {})
                article = ctx.get("article", "")
                product_name = ctx.get("name", "")
                size = ctx.get("size", "")
                if article and size:
                    await wishlist_add(chat_id, user_name, article, product_name, size)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Записал вас в лист ожидания на {product_name} размер {size} — напишу как только появится 😊"
                    )

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
    """Команды владельца: /resume chat_id, /addphoto артикул, /deletephoto артикул."""
    text = update.message.text or ""

    # /resume
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

    # /addphoto артикул
    elif text.startswith("/addphoto"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await update.message.reply_text(
                "Использование: /addphoto АРТИКУЛ\n"
                "Пример: /addphoto BAPE-001\n\n"
                "После команды кидай фото подряд — все сохранятся."
            )
            return
        article = parts[1].strip().upper()
        owner_chat_id = update.effective_chat.id
        pending_photo_article[owner_chat_id] = article
        # Показываем сколько фото уже есть
        existing = await get_product_photos(article)
        count_text = f"Уже есть: {len(existing)} фото." if existing else "Фото ещё нет."
        await update.message.reply_text(
            f"📸 Жду фото для артикула {article}\n"
            f"{count_text}\n\n"
            f"Кидай фото — сохраню все. Когда закончишь, напиши /done"
        )

    # /deletephoto артикул
    elif text.startswith("/deletephoto"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await update.message.reply_text("Использование: /deletephoto АРТИКУЛ")
            return
        article = parts[1].strip().upper()
        deleted = await delete_product_photos(article)
        await update.message.reply_text(f"🗑 Удалено {deleted} фото для артикула {article}")

    # /notify артикул размер — уведомить вишлист
    elif text.startswith("/notify"):
        parts = text.split()
        if len(parts) < 3:
            await update.message.reply_text("Использование: /notify АРТИКУЛ РАЗМЕР\nПример: /notify BAPE-001 L")
            return
        article = parts[1].upper()
        size = parts[2].upper()
        count = await wishlist_notify(context.bot, article, size)
        await update.message.reply_text(f"✅ Уведомлено {count} покупателей из вишлиста ({article} / {size})")

    # /wishlist — показать весь список ожидания
    elif text.startswith("/wishlist"):
        if not db_pool:
            await update.message.reply_text("БД недоступна")
            return
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT product_name, size, user_name, chat_id FROM wishlist ORDER BY product_name, size")
        if not rows:
            await update.message.reply_text("Вишлист пуст")
            return
        lines = [f"• {r['product_name']} / {r['size']} — {r['user_name']} (id:{r['chat_id']})" for r in rows]
        await update.message.reply_text("📋 Вишлист:\n" + "\n".join(lines))
        owner_chat_id = update.effective_chat.id
        if owner_chat_id in pending_photo_article:
            article = pending_photo_article.pop(owner_chat_id)
            photos = await get_product_photos(article)
            await update.message.reply_text(
                f"✅ Загрузка завершена!\n"
                f"Артикул: {article}\n"
                f"Всего фото: {len(photos)}"
            )
        else:
            await update.message.reply_text("Нет активной загрузки фото.")

    # /listphotos артикул
    elif text.startswith("/listphotos"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await update.message.reply_text("Использование: /listphotos АРТИКУЛ")
            return
        article = parts[1].strip().upper()
        photos = await get_product_photos(article)
        if not photos:
            await update.message.reply_text(f"❌ Фото для {article} не найдены")
            return
        await update.message.reply_text(f"📸 Фото для {article}: {len(photos)} шт. Показываю...")
        media = [{"type": "photo", "media": fid} for fid in photos[:10]]
        try:
            from telegram import InputMediaPhoto
            await context.bot.send_media_group(
                chat_id=update.effective_chat.id,
                media=[InputMediaPhoto(media=fid) for fid in photos[:10]]
            )
        except Exception as e:
            await update.message.reply_text(f"Ошибка показа фото: {e}")


async def handle_owner_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Владелец прислал фото — скачиваем и перезаливаем через клиентский бот."""
    owner_chat_id = update.effective_chat.id
    if owner_chat_id not in pending_photo_article:
        return

    article = pending_photo_article[owner_chat_id]

    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        photo_bytes = await file.download_as_bytearray()

        client_token = os.getenv("CLIENT_BOT_TOKEN")
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.post(
                f"https://api.telegram.org/bot{client_token}/sendPhoto",
                params={"chat_id": OWNER_CHAT_ID},
                files={"photo": ("photo.jpg", bytes(photo_bytes), "image/jpeg")}
            )
            data = resp.json()

        if not data.get("ok"):
            await update.message.reply_text(f"❌ Ошибка загрузки: {data.get('description')}")
            return

        client_file_id = data["result"]["photo"][-1]["file_id"]

        # Удаляем служебное сообщение
        try:
            msg_id = data["result"]["message_id"]
            async with httpx.AsyncClient(timeout=5) as http:
                await http.post(
                    f"https://api.telegram.org/bot{client_token}/deleteMessage",
                    params={"chat_id": OWNER_CHAT_ID, "message_id": msg_id}
                )
        except Exception:
            pass

        # Берём описание из подписи к фото если есть
        caption = update.message.caption or ""

        success = await save_product_photo(article, client_file_id)
        if success:
            photos = await get_product_photos(article)
            await update.message.reply_text(
                f"✅ Фото сохранено! Артикул: {article} — всего {len(photos)} фото\n"
                f"Кидай ещё или напиши /done"
            )
        else:
            await update.message.reply_text("❌ Ошибка сохранения в БД")

    except Exception as e:
        print(f"[owner_photo] error: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


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
        client_app.add_handler(CallbackQueryHandler(handle_confirm, pattern="^confirm_"))
        client_app.add_handler(MessageHandler(filters.Chat(HQ_CHAT_ID) & filters.TEXT, handle_hq_message))
        client_app.add_handler(MessageHandler(filters.VOICE, handle_voice))
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
            owner_app.add_handler(CommandHandler("addphoto", handle_owner_commands))
            owner_app.add_handler(CommandHandler("deletephoto", handle_owner_commands))
            owner_app.add_handler(CommandHandler("listphotos", handle_owner_commands))
            owner_app.add_handler(CommandHandler("done", handle_owner_commands))
            owner_app.add_handler(CommandHandler("notify", handle_owner_commands))
            owner_app.add_handler(CommandHandler("wishlist", handle_owner_commands))
            owner_app.add_handler(MessageHandler(filters.PHOTO, handle_owner_photo))
            owner_app.add_handler(MessageHandler(filters.TEXT & filters.REPLY, handle_owner_reply))

            # /post — через content_agent
            try:
                from content_agent import cmd_post as handle_post_cmd, handle_callback as handle_pub_callback
                owner_app.add_handler(CommandHandler("post", handle_post_cmd))
                owner_app.add_handler(CallbackQueryHandler(handle_pub_callback, pattern="^pub_"))
            except Exception as e:
                print(f"[owner] content_agent import error: {e}")

            await owner_app.initialize()
            await owner_app.start()
            await owner_app.updater.start_polling(drop_pending_updates=True)
            print("Оба бота запущены")

            # Московское время
            from datetime import timezone, timedelta
            msk = timezone(timedelta(hours=3))
            now = datetime.now(msk).strftime("%d.%m.%Y %H:%M")

            # Стартовое сообщение владельцу
            try:
                owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
                async with httpx.AsyncClient(timeout=5) as http:
                    await http.post(
                        f"https://api.telegram.org/bot{owner_token}/sendMessage",
                        json={
                            "chat_id": OWNER_CHAT_ID,
                            "text": (
                                f"🚀 KROSHIDE AI technologies development запущен\n\n"
                                f"Агенты на линии:\n"
                                f"• Тёма — менеджер покупателей (@oqwiendowqej3213_bot)\n"
                                f"• Тимка — бухгалтер и аналитик (@tema22_38bot)\n"
                                f"• Контент-агент — постинг в канал (@irioqwqhdqdiw12332_bot)\n\n"
                                f"Время запуска: {now} МСК"
                            )
                        }
                    )
            except Exception as e:
                print(f"[startup] notify error: {e}")

            # Сообщение в HQ чат (General тема)
            try:
                client_token = os.getenv("CLIENT_BOT_TOKEN")
                async with httpx.AsyncClient(timeout=5) as http:
                    await http.post(
                        f"https://api.telegram.org/bot{client_token}/sendMessage",
                        json={
                            "chat_id": HQ_CHAT_ID,
                            "text": (
                                f"🚀 KROSHIDE AI technologies development запущен\n\n"
                                f"Агенты на линии:\n"
                                f"• Тёма — менеджер покупателей\n"
                                f"• Тимка — бухгалтер и аналитик\n"
                                f"• Контент-агент — постинг в канал\n\n"
                                f"Время: {now} МСК"
                            )
                        }
                    )
            except Exception as e:
                print(f"[startup] hq notify error: {e}")
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
