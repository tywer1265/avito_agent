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
        return inv_text, items

    try:
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.get(N8N_INVENTORY_URL)
            data = resp.json()
            if isinstance(data, list):
                items = [i for i in data if int(i.get("stock", 0)) > 0]
                lines = [
                    f"- {i['name']} ({i['size']}): {i['price']}₽, остаток: {i['stock']} шт"
                    + (" ⚠️ ПОСЛЕДНИЙ" if int(i.get('stock', 99)) <= 2 else "")
                    for i in items
                ]
                inv_text = "\n".join(lines)
                _inv_cache = (inv_text, items, datetime.now().timestamp())
    except Exception as e:
        print(f"[content] inventory error: {e}")

    return inv_text, items


async def get_photos(article: str) -> list:
    """Берём фото товара из PostgreSQL через tg_agent."""
    # Импортируем функцию из tg_agent если доступна
    try:
        import sys
        if 'tg_agent' in sys.modules:
            from tg_agent import get_product_photos
            return await get_product_photos(article)
    except Exception:
        pass
    return []

# ── Расписание ──────────────────────────────────────────────────

def get_daily_schedule() -> list:
    """5 постов в день с рандомным порядком и временем."""
    windows = [10, 13, 16, 19, 21]
    random.shuffle(windows)

    types = ["drop", "drop", "outfit", "sold", "brand_fact"]

    # Анекдот раз в 3 дня
    day = datetime.now().timetuple().tm_yday
    if day % 3 == 0:
        replace_idx = random.randint(2, 4)
        types[replace_idx] = "joke"

    random.shuffle(types)

    # Добавляем рандомные минуты
    schedule = [(h, random.randint(0, 59), t) for h, t in zip(windows, types)]
    return sorted(schedule, key=lambda x: x[0])

# ── Генерация постов ────────────────────────────────────────────

async def generate_post(post_type: str, article: str = "") -> dict:
    inv_text, items = await get_inventory()
    if not items:
        return {}

    # Выбираем товар
    if article:
        item = next((i for i in items if str(i.get("article", "")).upper() == article.upper()), None)
        if not item:
            item = random.choice(items)
    else:
        item = random.choice(items)

    is_last = int(item.get("stock", 99)) <= 2
    photos = await get_photos(str(item.get("article", "")))

    prompts = {
        "drop": f"""Напиши пост-дроп для TG канала магазина KROSHIDE.

Товар: {item.get('name')}
Размер: {item.get('size')}
Цена: {item.get('price')}₽
{"Остался последний!" if is_last else ""}

Формат (строго):
🔥 ДРОП — KROSHIDE

{item.get('name')}
Размер {item.get('size')}{"· Остался последний" if is_last else ""}

[2-3 строки про качество и стиль. Дерзко, коротко, без воды.]

{item.get('price')} ₽ · Отправка по всему миру

Забрать → {CHANNEL_USERNAME}

#[5-7 хэштегов]

Только текст поста без пояснений.""",

        "outfit": f"""Напиши пост "образ дня" для TG канала KROSHIDE.
Основа образа: {item.get('name')} — {item.get('price')}₽

Склад (подбери 1-2 дополнительные вещи):
{inv_text[:800]}

Формат:
🖤 ОБРАЗ ДНЯ — KROSHIDE

[товар 1] + [товар 2] + [товар 3]
[итоговая цена] ₽

[1-2 строки про стиль образа]

Собрать образ → {CHANNEL_USERNAME}

#streetwear #kroshide #outfit [ещё 3-4 хэштега]

Только текст поста.""",

        "sold": f"""Напиши пост "только что продали" для TG канала KROSHIDE.
Товар: {item.get('name')}, размер {item.get('size')}

Формат:
✅ ТОЛЬКО ЧТО УЛЕТЕЛ

{item.get('name')} · {item.get('size')}
[короткая фраза про популярность или остаток]

Успей → {CHANNEL_USERNAME}

#kroshide #streetwear [2-3 хэштега]

Только текст поста.""",

        "brand_fact": f"""Напиши пост "интересный факт" для TG канала KROSHIDE.
Бренд: {item.get('name', '').split()[0] if item.get('name') else 'Bape'}
Наш товар: {item.get('name')} — {item.get('price')}₽

Формат:
🧠 А ТЫ ЗНАЛ?

[реальный интересный исторический факт про этот бренд. 2-3 предложения.]

[название нашего товара] — {item.get('price')} ₽
{CHANNEL_USERNAME}

#[бренд] #streetwear #kroshide #факт

Только текст поста.""",

        "joke": f"""Найди очень смешной короткий анекдот. Реально смешной, не банальный, желательно про моду или деньги.

Формат:
😂 Анекдот дня

[анекдот 3-5 строк]

А одежда по хорошим ценам — только у нас 😏
{CHANNEL_USERNAME}

#юмор #анекдот #kroshide

Только текст поста."""
    }

    try:
        loop = asyncio.get_event_loop()
        prompt = prompts.get(post_type, prompts["drop"])

        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

        response = await loop.run_in_executor(None, _call)
        text = response.content[0].text.strip()

        return {
            "type": post_type,
            "text": text,
            "photo_file_id": photos[0] if photos else None,
            "article": str(item.get("article", "")),
            "item_name": str(item.get("name", ""))
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
        if post.get("photo_file_id"):
            msg = await bot.send_photo(
                chat_id=OWNER_CHAT_ID,
                photo=post["photo_file_id"],
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
        print(f"[content] пост на апруве: {post['type']} msg={msg.message_id}")

        # Автопубликация через 10 минут если нет ответа
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
            if post.get("photo_file_id"):
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendPhoto",
                    json={
                        "chat_id": CHANNEL_ID,
                        "photo": post["photo_file_id"],
                        "caption": post["text"][:1024]
                    }
                )
            else:
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": CHANNEL_ID,
                        "text": post["text"][:4096]
                    }
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
    """/post [тип] [артикул] — сгенерировать пост вручную."""
    if update.effective_user.id != OWNER_CHAT_ID:
        return

    type_map = {
        "дроп": "drop", "drop": "drop",
        "образ": "outfit", "outfit": "outfit",
        "анекдот": "joke", "joke": "joke",
        "факт": "brand_fact",
        "продажа": "sold", "sold": "sold"
    }

    args = context.args or []
    post_type = type_map.get(args[0].lower(), "drop") if args else "drop"
    article = args[1] if len(args) > 1 else (args[0] if args and args[0].upper() == args[0] else "")

    await update.message.reply_text(f"🎨 Генерирую {post_type}...")
    post = await generate_post(post_type, article)
    if post:
        await send_for_approval(context.bot, post)
    else:
        await update.message.reply_text("❌ Не смог сгенерировать пост")


# ── Запуск ───────────────────────────────────────────────────────

async def run():
    """Просто запускаем планировщик — без polling."""
    # Создаём бота только для отправки сообщений
    from telegram import Bot
    bot = Bot(token=CONTENT_BOT_TOKEN)
    print("[content] бот @irioqwqhdqdiw12332_bot инициализирован")
    asyncio.create_task(run_scheduler(bot))
    # Держим живым
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
