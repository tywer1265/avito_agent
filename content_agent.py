"""
content_agent.py — KROSHIDE Content Agent
Автоматически генерирует и постит контент в TG канал @kroshide
Запускается из main.py в отдельном потоке
"""
import os
import asyncio
import random
import httpx
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import anthropic

# ── Конфиг ─────────────────────────────────────────────────────
CONTENT_BOT_TOKEN = os.getenv("CONTENT_BOT_TOKEN", "8952442767:AAHM8mS54O4JA029vFw1goC1QOVx07Fa-7E")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
CHANNEL_ID = -1004293368595       # @kroshide
CHANNEL_USERNAME = "@kroshidemanager"
N8N_INVENTORY_URL = "https://tywer1265.app.n8n.cloud/webhook/inventory"
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")

client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# Состояние
pending_posts: dict = {}   # msg_id → post dict
_inv_cache: tuple = ("", [], 0.0)

# ── Склад ───────────────────────────────────────────────────────

async def get_inventory() -> tuple:
    global _inv_cache
    inv_text, items, ts = _inv_cache
    if inv_text and (datetime.now().timestamp() - ts) < 300:
        print(f"[content] склад из кэша: {len(items)} товаров")
        return inv_text, items

    try:
        print(f"[content] запрашиваю склад из n8n...")
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.get(N8N_INVENTORY_URL)
            data = resp.json()
            # n8n возвращает {"inventory": [...]} или просто [...]
            if isinstance(data, dict):
                data = data.get("inventory", data.get("items", []))
            print(f"[content] склад получен: {len(data) if isinstance(data, list) else 'не список'} записей")
            if isinstance(data, list):
                items = [i for i in data if int(i.get("stock", 0)) > 0]
                print(f"[content] в наличии: {len(items)} товаров")
                lines = [
                    f"- {i['name']} ({i['size']}): {i['price']}₽, остаток: {i['stock']} шт"
                    + (" ⚠️ ПОСЛЕДНИЙ" if int(i.get('stock', 99)) <= 2 else "")
                    for i in items
                ]
                inv_text = "\n".join(lines)
                _inv_cache = (inv_text, items, datetime.now().timestamp())
            else:
                print(f"[content] склад вернул не список: {str(data)[:200]}")
    except Exception as e:
        print(f"[content] inventory error: {e}")

    return inv_text, items


async def get_photos(article: str) -> list:
    """Берём фото товара напрямую из PostgreSQL."""
    import asyncpg
    db_url = os.getenv("DATABASE_URL_ASYNCPG", os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://"))
    if not db_url:
        return []
    try:
        cyrillic_to_latin = str.maketrans("АВСЕКМНОРТХавсекмнорТх", "ABCEKMHOPTXabcekmhoptx")
        article_norm = article.strip().upper().translate(cyrillic_to_latin)
        conn = await asyncpg.connect(db_url)
        rows = await conn.fetch(
            "SELECT file_id FROM product_photos WHERE UPPER(article) = $1 ORDER BY created_at ASC",
            article_norm
        )
        await conn.close()
        return [r["file_id"] for r in rows]
    except Exception as e:
        print(f"[content] get_photos error: {e}")
        return []

# ── Расписание ──────────────────────────────────────────────────

def get_daily_schedule() -> list:
    """5 постов в день с рандомным порядком и временем."""
    windows = [10, 13, 16, 19, 21]
    random.shuffle(windows)

    # Пул типов — с весами
    pool = [
        "drop", "drop", "drop",        # дропы чаще
        "stock", "stock",               # товары со склада
        "outfit",                       # образ
        "hype",                         # хайп
        "urgency",                      # срочность
        "price_comparison",             # сравнение цен
        "sold",                         # продажа
        "brand_fact",                   # факт
        "lifestyle",                    # лайфстайл
        "social_proof",                 # соцдоказательство
    ]

    # Анекдот раз в 3 дня, вопрос раз в 5 дней
    day = datetime.now().timetuple().tm_yday
    if day % 3 == 0:
        pool.append("joke")
    if day % 5 == 0:
        pool.append("question")
    if day % 7 == 0:
        pool.append("behind_scenes")

    types = random.sample(pool, min(5, len(pool)))
    random.shuffle(types)

    schedule = [(h, random.randint(5, 55), t) for h, t in zip(windows, types)]
    return sorted(schedule, key=lambda x: x[0])

# ── Генерация постов ────────────────────────────────────────────

async def generate_post(post_type: str, article: str = "") -> dict:
    inv_text, items = await get_inventory()
    if not items and post_type != "random":
        return {}

    # Выбираем товар
    available = items
    item = None
    if post_type in ["stock"] or article:
        if article:
            item = next((i for i in available if str(i.get("article","")).upper() == article.upper()), None)
        if not item and available:
            item = random.choice(available)
    elif available:
        item = random.choice(available)

    is_last = int(item.get("stock", 99)) <= 2 if item else False
    photos = await get_photos(str(item.get("article", ""))) if item else []

    # Для random — выбираем случайный тип из всего арсенала
    ALL_TYPES = [
        "drop", "drop", "drop",           # дропы чаще всего
        "outfit", "outfit",                # образы
        "sold",                            # продажи
        "brand_fact",                      # факты
        "hype",                            # хайп пост
        "price_comparison",                # сравнение цен
        "lifestyle",                       # лайфстайл
        "urgency",                         # срочность
        "social_proof",                    # соцдоказательство
        "joke",                            # анекдот
        "behind_scenes",                   # за кулисами
        "question",                        # вопрос аудитории
    ]

    if post_type == "random":
        post_type = random.choice(ALL_TYPES)
    elif post_type == "stock":
        post_type = random.choice(["drop", "drop", "outfit", "sold", "urgency"])

    item_name = item.get('name', '') if item else ''
    item_size = item.get('size', '') if item else ''
    item_price = item.get('price', '') if item else ''
    brand = item_name.split()[0] if item_name else 'KROSHIDE'

    MASTER_SYSTEM = f"""Ты — лучший SMM-специалист в мире по streetwear.
Ты создаёшь контент для TG канала магазина KROSHIDE.
Мы продаём премиум реплики Bape, CDG, Y3, Гоша Рубчинский, Mastermind.

ПРАВИЛА:
- Пиши живо, дерзко, по-молодёжному. Никакого официоза.
- Цепляй с первой строки. Первые 2 слова решают всё.
- Используй психологию продаж: дефицит, срочность, социальное доказательство, FOMO.
- Хэштеги всегда в конце, 5-8 штук. Без пробела между # и словом.
- Ссылка на менеджера всегда: {CHANNEL_USERNAME}
- Максимум 8 строк текста без хэштегов.
- Никогда не используй слово "реплика" открыто в тексте.
- Пиши только текст поста. Ноль пояснений."""

    prompts = {

        "drop": f"""Напиши убойный пост-дроп.
Товар: {item_name} · Размер {item_size} · {item_price}₽{"· ПОСЛЕДНИЙ" if is_last else ""}

Варианты открытия (выбери один или придумай лучше):
- "🔥 ДРОП — KROSHIDE"
- "Это улетит сегодня 👀"
- "Пришло. Берёшь?"
- "Не успеешь — пожалеешь"

Описание: качество, материал, почему это must-have. 2-3 строки.
Цена и доставка: {item_price}₽ · По всему миру
Контакт: {CHANNEL_USERNAME}
Хэштеги про {brand} и streetwear.""",

        "outfit": f"""Напиши пост "образ дня".
Основа: {item_name} · {item_price}₽
Склад для подбора: {inv_text[:600]}

Подбери 2-3 вещи которые реально сочетаются. Опиши образ как стилист.
Покажи итоговую цену за всё.
Контакт: {CHANNEL_USERNAME}""",

        "sold": f"""Напиши пост "только что ушло".
Товар: {item_name} · {item_size}

Варианты:
- "✅ УЛЕТЕЛО"
- "Не успел? Жаль."
- "Ещё один счастливчик"

Создай ощущение что товар популярный и его быстро разбирают.
Намекни что похожее ещё есть. {CHANNEL_USERNAME}""",

        "brand_fact": f"""Напиши пост "факт о бренде {brand}".
Наш товар: {item_name} · {item_price}₽

Найди реально интересный малоизвестный факт про {brand}.
Должно быть "вау, не знал". Не банальщина.
Свяжи с нашим товаром в конце. {CHANNEL_USERNAME}""",

        "hype": f"""Напиши хайп-пост про {brand} без прямой продажи.
Товар в наличии: {item_name} · {item_price}₽

Создай ажиотаж вокруг бренда. Почему все его хотят.
Кто его носит (знаменитости, субкультуры). Почему это статус.
В конце мягко подведи к нашему каналу. {CHANNEL_USERNAME}""",

        "price_comparison": f"""Напиши пост "наша цена vs оригинал".
Товар: {item_name} · Наша цена: {item_price}₽

Найди реальную цену оригинала {brand} в интернете (обычно в 8-15 раз дороже).
Покажи разницу наглядно. Задай риторический вопрос.
Без слова "реплика". {CHANNEL_USERNAME}""",

        "lifestyle": f"""Напиши лайфстайл пост про streetwear культуру.
Связь с товаром: {item_name}

Про стиль жизни, самовыражение, почему streetwear это больше чем одежда.
Философски, но коротко. Цепляй эмоцию. {CHANNEL_USERNAME}""",

        "urgency": f"""Напиши пост с максимальной срочностью.
Товар: {item_name} · {item_size} · {item_price}₽{"· ОСТАЛСЯ ПОСЛЕДНИЙ" if is_last else " · заканчивается"}

Создай максимальный FOMO. Таймер в голове у читателя.
Дефицит, уникальность, сейчас или никогда.
{CHANNEL_USERNAME}""",

        "social_proof": f"""Напиши пост с социальным доказательством.
Товар: {item_name}

Покупают уже N-й человек на этой неделе (придумай реалистичную цифру).
Скрин отзыва (придумай реалистичный отзыв покупателя).
Почему люди возвращаются. {CHANNEL_USERNAME}""",

        "joke": """Найди самый смешной анекдот про моду, деньги или стиль.
Реально смешной, не баян.

Формат:
😂 Анекдот дня

[анекдот]

А одежда по хорошим ценам — только у нас 😏
@kroshidemanager

#юмор #мода #kroshide""",

        "behind_scenes": f"""Напиши пост "за кулисами" магазина.
Товар который готовим: {item_name}

Покажи процесс: отбор товара, проверка качества, упаковка.
Дай ощущение что за магазином стоят люди которые реально разбираются.
Создай доверие. {CHANNEL_USERNAME}""",

        "question": f"""Напиши интерактивный пост-вопрос для вовлечения аудитории.
Связь с товаром: {item_name} · {brand}

Задай цепляющий вопрос про стиль, моду или streetwear.
Попроси ответить в комментариях или реакцией.
В конце упомяни что у нас есть крутые вещи. {CHANNEL_USERNAME}""",
    }

    try:
        loop = asyncio.get_event_loop()
        prompt = prompts.get(post_type, prompts["drop"])

        def _call():
            return client.messages.create(
                model="claude-sonnet-4-6",  # Sonnet для лучшего качества постов
                max_tokens=600,
                system=MASTER_SYSTEM,
                messages=[{"role": "user", "content": prompt}]
            )

        response = await loop.run_in_executor(None, _call)
        text = response.content[0].text.strip()

        return {
            "type": post_type,
            "text": text,
            "photo_file_id": photos[0] if photos else None,
            "article": str(item.get("article", "")) if item else "",
            "item_name": item_name
        }

    except Exception as e:
        print(f"[content] generate error: {e}")
        return {}

# ── Апрув и публикация ──────────────────────────────────────────

async def send_for_approval(bot, post: dict) -> None:
    if not post:
        return

    labels = {
        "drop": "🔥 Дроп",
        "outfit": "🖤 Образ дня",
        "sold": "✅ Продажа",
        "brand_fact": "🧠 Факт",
        "joke": "😂 Анекдот"
    }

    caption = (
        f"📢 Пост для канала\n"
        f"Тип: {labels.get(post['type'], post['type'])}\n"
        f"Товар: {post.get('item_name', '—')}\n\n"
        f"{post['text']}"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Опубликовать", callback_data="pub_yes"),
        InlineKeyboardButton("✏️ Переделать",  callback_data="pub_redo"),
        InlineKeyboardButton("❌ Отмена",       callback_data="pub_no")
    ]])

    try:
        photo_bytes = None

        # Скачиваем фото через клиентский бот если есть file_id
        if post.get("photo_file_id"):
            try:
                client_token = os.getenv("CLIENT_BOT_TOKEN")
                async with httpx.AsyncClient(timeout=15) as http:
                    # Получаем ссылку на файл
                    r = await http.get(
                        f"https://api.telegram.org/bot{client_token}/getFile",
                        params={"file_id": post["photo_file_id"]}
                    )
                    file_data = r.json()
                    if file_data.get("ok"):
                        file_path = file_data["result"]["file_path"]
                        img_resp = await http.get(
                            f"https://api.telegram.org/file/bot{client_token}/{file_path}"
                        )
                        photo_bytes = img_resp.content
            except Exception as e:
                print(f"[content] photo download error: {e}")

        if photo_bytes:
            msg = await bot.send_photo(
                chat_id=OWNER_CHAT_ID,
                photo=photo_bytes,
                caption=caption[:1024],
                reply_markup=keyboard
            )
        else:
            msg = await bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=caption[:4096],
                reply_markup=keyboard
            )

        pending_posts[msg.message_id] = post
        # Сохраняем байты фото для публикации
        if photo_bytes:
            pending_posts[msg.message_id]["photo_bytes"] = photo_bytes

        print(f"[content] пост на апруве: {post['type']} msg={msg.message_id}")
        asyncio.create_task(_auto_publish(bot, msg.message_id, post))

    except Exception as e:
        print(f"[content] approval error: {e}")


async def _auto_publish(bot, msg_id: int, post: dict) -> None:
    await asyncio.sleep(600)
    if msg_id in pending_posts:
        ok = await publish(post)
        del pending_posts[msg_id]
        status = "✅ Опубликован автоматически (апрув не получен)" if ok else "❌ Ошибка автопубликации"
        await bot.send_message(chat_id=OWNER_CHAT_ID, text=status)


async def publish(post: dict) -> bool:
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            photo_bytes = post.get("photo_bytes")
            if photo_bytes:
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendPhoto",
                    params={"chat_id": CHANNEL_ID},
                    files={"photo": ("photo.jpg", photo_bytes, "image/jpeg")},
                    data={"caption": post["text"][:1024]}
                )
            else:
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendMessage",
                    json={"chat_id": CHANNEL_ID, "text": post["text"][:4096]}
                )
            data = resp.json()
            if data.get("ok"):
                print(f"[content] опубликовано: {post.get('type')}")
                return True
            else:
                print(f"[content] publish failed: {data.get('description')}")
                return False
    except Exception as e:
        print(f"[content] publish error: {e}")
        return False

# ── Планировщик ─────────────────────────────────────────────────

async def run_scheduler(bot) -> None:
    print("[content] планировщик запущен")
    while True:
        try:
            now = datetime.now()
            schedule = get_daily_schedule()
            print(f"[content] расписание на сегодня: {[(f'{h}:{m:02d}', t) for h,m,t in schedule]}")

            for hour, minute, post_type in schedule:
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target <= now:
                    continue

                wait = (target - now).total_seconds()
                print(f"[content] следующий пост: {post_type} в {hour}:{minute:02d} (через {wait/60:.0f} мин)")
                await asyncio.sleep(wait)

                post = await generate_post(post_type)
                if post:
                    await send_for_approval(bot, post)

                now = datetime.now()

            # Ждём до 09:55 следующего дня
            tomorrow = now.replace(hour=9, minute=55, second=0, microsecond=0)
            from datetime import timedelta
            if tomorrow <= now:
                tomorrow += timedelta(days=1)
            wait = (tomorrow - now).total_seconds()
            print(f"[content] все посты на сегодня готовы, следующий цикл через {wait/3600:.1f}ч")
            await asyncio.sleep(max(wait, 60))

        except asyncio.CancelledError:
            print("[content] планировщик остановлен")
            break
        except Exception as e:
            print(f"[content] scheduler error: {e}")
            await asyncio.sleep(300)

# ── Хендлеры бота ───────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    msg_id = query.message.message_id
    post = pending_posts.get(msg_id)

    if query.data == "pub_yes":
        if post:
            ok = await publish(post)
            del pending_posts[msg_id]
            await query.edit_message_reply_markup(reply_markup=None)
            text = "✅ Пост опубликован в @kroshide" if ok else "❌ Ошибка публикации"
            await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=text)
        else:
            await context.bot.send_message(chat_id=OWNER_CHAT_ID, text="❌ Пост не найден (истёк?)")

    elif query.data == "pub_redo":
        if post:
            del pending_posts[msg_id]
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text="🔄 Генерирую новый вариант...")
        article = post.get("article", "") if post else ""
        post_type = post.get("type", "drop") if post else "drop"
        new_post = await generate_post(post_type, article)
        if new_post:
            await send_for_approval(context.bot, new_post)

    elif query.data == "pub_no":
        if msg_id in pending_posts:
            del pending_posts[msg_id]
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text="❌ Пост отменён")


async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/post random | /post stock | /post [тип] | /post [артикул]"""
    if update.effective_user.id != OWNER_CHAT_ID:
        return

    args = context.args or []
    arg = args[0].lower() if args else "random"

    type_map = {
        "random": "random",
        "stock": "stock",
        "дроп": "drop", "drop": "drop",
        "образ": "outfit", "outfit": "outfit",
        "анекдот": "joke", "joke": "joke",
        "факт": "brand_fact",
        "продажа": "sold", "sold": "sold",
        "хайп": "hype", "hype": "hype",
        "цена": "price_comparison",
        "лайф": "lifestyle",
        "срочно": "urgency",
        "отзыв": "social_proof",
        "кулисы": "behind_scenes",
        "вопрос": "question",
    }

    post_type = type_map.get(arg, "random")
    # Если передали артикул (заглавные буквы или цифры) — делаем дроп по артикулу
    article = args[0] if args and args[0].upper() == args[0] and arg not in type_map else ""
    if article:
        post_type = "stock"

    labels = {
        "random": "🎲 Рандом",
        "stock": "📦 Товар со склада",
        "drop": "🔥 Дроп", "outfit": "🖤 Образ",
        "sold": "✅ Продажа", "brand_fact": "🧠 Факт",
        "hype": "💥 Хайп", "price_comparison": "💰 Сравнение цен",
        "lifestyle": "🌊 Лайфстайл", "urgency": "⏰ Срочность",
        "social_proof": "👥 Соцдоказательство",
        "joke": "😂 Анекдот", "behind_scenes": "🎬 За кулисами",
        "question": "❓ Вопрос"
    }

    await update.message.reply_text(f"🎨 Генерирую: {labels.get(post_type, post_type)}...")
    post = await generate_post(post_type, article)
    if post:
        await send_for_approval(context.bot, post)
    else:
        await update.message.reply_text("❌ Не смог сгенерировать пост. Проверь склад.")


# ── Запуск ───────────────────────────────────────────────────────

async def run():
    """Просто запускаем планировщик — без polling."""
    from telegram import Bot
    from datetime import timezone, timedelta
    bot = Bot(token=CONTENT_BOT_TOKEN)
    print("[content] бот @irioqwqhdqdiw12332_bot инициализирован")

    # Пишем в HQ что контент-агент на линии
    try:
        msk = timezone(timedelta(hours=3))
        now = datetime.now(msk).strftime("%d.%m.%Y %H:%M")
        owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(
                f"https://api.telegram.org/bot{owner_token}/sendMessage",
                json={
                    "chat_id": -1004385799918,
                    "text": f"📢 Контент-агент на линии · {now} МСК\nПостинг в @kroshide активен · 5 постов в день"
                }
            )
    except Exception as e:
        print(f"[content] hq notify error: {e}")

    asyncio.create_task(run_scheduler(bot))
    await asyncio.Event().wait()


def start_content_agent():
    """Запуск из main.py в отдельном потоке."""
    import threading

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    print("[content] content_agent поток запущен")
