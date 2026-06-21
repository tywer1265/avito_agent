"""
content_agent.py — KROSHIDE Content Agent
Постит товары из склада в TG канал @kroshide
3-5 постов в день. Только товары. Минималистично.
"""
import os
import asyncio
import random
import httpx
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

# ── Конфиг ─────────────────────────────────────────────────────
CONTENT_BOT_TOKEN = os.getenv("CONTENT_BOT_TOKEN", "8952442767:AAHM8mS54O4JA029vFw1goC1QOVx07Fa-7E")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
CHANNEL_ID = -1004293368595
CHANNEL_USERNAME = "@kroshidemanager"
N8N_INVENTORY_URL = "https://tywer1265.app.n8n.cloud/webhook/inventory"
MSK = timezone(timedelta(hours=3))

# Состояние
pending_posts: dict = {}
_inv_cache: tuple = ("", [], 0.0)
posted_today: set = set()  # артикулы которые уже постили сегодня

# ── Склад ───────────────────────────────────────────────────────

async def get_inventory() -> list:
    global _inv_cache
    _, items, ts = _inv_cache
    if items and (datetime.now().timestamp() - ts) < 300:
        return items
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.get(N8N_INVENTORY_URL)
            data = resp.json()
            if isinstance(data, dict):
                data = data.get("inventory", data.get("items", []))
            if isinstance(data, list):
                items = [i for i in data if int(i.get("stock", 0)) > 0]
                _inv_cache = ("ok", items, datetime.now().timestamp())
                print(f"[content] склад: {len(items)} товаров в наличии")
                return items
    except Exception as e:
        print(f"[content] inventory error: {e}")
    return []


async def get_photos(article: str) -> list:
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


async def download_photo(file_id: str) -> bytes | None:
    """Скачиваем фото через клиентского бота."""
    try:
        client_token = os.getenv("CLIENT_BOT_TOKEN")
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.get(
                f"https://api.telegram.org/bot{client_token}/getFile",
                params={"file_id": file_id}
            )
            file_data = r.json()
            if file_data.get("ok"):
                file_path = file_data["result"]["file_path"]
                img = await http.get(f"https://api.telegram.org/file/bot{client_token}/{file_path}")
                return img.content
    except Exception as e:
        print(f"[content] download error: {e}")
    return None

# ── Генерация поста ─────────────────────────────────────────────

def build_post_text(item: dict) -> str:
    """Минималистичный пост про товар."""
    name = item.get("name", "")
    size = item.get("size", "")
    price = item.get("price", "")
    stock = int(item.get("stock", 99))

    # Эмодзи по бренду
    brand_emoji = {
        "bape": "🦍", "cdg": "❤️", "y3": "⚡️",
        "гоша": "🔥", "gosha": "🔥", "mastermind": "💀"
    }
    emoji = "🔥"
    for key, val in brand_emoji.items():
        if key in name.lower():
            emoji = val
            break

    # Срочность если последний
    urgency = "\nПоследний размер 👀" if stock <= 2 else ""

    text = (
        f"{emoji} {name} · {size} · {price}₽"
        f"{urgency}\n\n"
        f"{CHANNEL_USERNAME}"
    )
    return text


async def generate_post(article: str = "") -> dict:
    """Генерируем пост по артикулу или рандомному товару."""
    items = await get_inventory()
    if not items:
        return {}

    # Выбираем товар
    if article:
        item = next((i for i in items if str(i.get("article", "")).upper() == article.upper()), None)
        if not item:
            return {}
    else:
        # Не постим то что уже постили сегодня
        available = [i for i in items if str(i.get("article", "")) not in posted_today]
        if not available:
            available = items  # если всё уже постили — берём любой
        item = random.choice(available)

    art = str(item.get("article", ""))
    photos = await get_photos(art)
    photo_bytes = await download_photo(photos[0]) if photos else None

    text = build_post_text(item)

    return {
        "text": text,
        "photo_bytes": photo_bytes,
        "article": art,
        "item_name": str(item.get("name", ""))
    }

# ── Расписание ──────────────────────────────────────────────────

def get_daily_schedule() -> list:
    """3-5 постов в день, рандомное время 10:00-22:00."""
    count = random.randint(3, 5)
    # Окна публикации
    all_hours = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    hours = sorted(random.sample(all_hours, count))
    schedule = [(h, random.randint(5, 55)) for h in hours]
    return schedule

# ── Апрув ───────────────────────────────────────────────────────

async def send_for_approval(bot: Bot, post: dict) -> None:
    if not post:
        return

    preview = f"📢 Пост для канала\nТовар: {post.get('item_name', '—')}\n\n{post['text']}"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Опубликовать", callback_data="pub_yes"),
        InlineKeyboardButton("🔄 Другой товар",  callback_data="pub_redo"),
        InlineKeyboardButton("❌ Пропустить",    callback_data="pub_no")
    ]])

    try:
        if post.get("photo_bytes"):
            msg = await bot.send_photo(
                chat_id=OWNER_CHAT_ID,
                photo=post["photo_bytes"],
                caption=preview[:1024],
                reply_markup=keyboard
            )
        else:
            msg = await bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=preview[:4096],
                reply_markup=keyboard
            )

        pending_posts[msg.message_id] = post
        print(f"[content] апрув: {post.get('item_name')} msg={msg.message_id}")
        asyncio.create_task(_auto_publish(bot, msg.message_id, post))

    except Exception as e:
        print(f"[content] approval error: {e}")


async def _auto_publish(bot: Bot, msg_id: int, post: dict) -> None:
    """Автопубликация если нет ответа 10 минут."""
    await asyncio.sleep(600)
    if msg_id in pending_posts:
        ok = await publish(post)
        del pending_posts[msg_id]
        status = "✅ Пост опубликован автоматически" if ok else "❌ Ошибка автопубликации"
        await bot.send_message(chat_id=OWNER_CHAT_ID, text=status)


async def publish(post: dict) -> bool:
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            if post.get("photo_bytes"):
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendPhoto",
                    params={"chat_id": CHANNEL_ID},
                    files={"photo": ("photo.jpg", post["photo_bytes"], "image/jpeg")},
                    data={"caption": post["text"][:1024]}
                )
            else:
                resp = await http.post(
                    f"https://api.telegram.org/bot{CONTENT_BOT_TOKEN}/sendMessage",
                    json={"chat_id": CHANNEL_ID, "text": post["text"][:4096]}
                )
            data = resp.json()
            if data.get("ok"):
                posted_today.add(post.get("article", ""))
                print(f"[content] опубликовано: {post.get('item_name')}")
                return True
            else:
                print(f"[content] publish failed: {data.get('description')}")
                return False
    except Exception as e:
        print(f"[content] publish error: {e}")
        return False

# ── Хендлеры ────────────────────────────────────────────────────

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
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text="✅ Опубликовано в @kroshide" if ok else "❌ Ошибка публикации"
            )

    elif query.data == "pub_redo":
        if msg_id in pending_posts:
            del pending_posts[msg_id]
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text="🔄 Генерирую другой товар...")
        new_post = await generate_post()
        if new_post:
            await send_for_approval(context.bot, new_post)

    elif query.data == "pub_no":
        if msg_id in pending_posts:
            del pending_posts[msg_id]
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text="❌ Пропущено")


async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/post — рандомный товар. /post A9 — конкретный артикул."""
    if update.effective_user.id != OWNER_CHAT_ID:
        return

    args = context.args or []
    article = args[0].upper() if args else ""

    await update.message.reply_text("📸 Генерирую пост...")
    post = await generate_post(article)
    if post:
        await send_for_approval(context.bot, post)
    else:
        await update.message.reply_text("❌ Товар не найден или нет фото")

# ── Планировщик ─────────────────────────────────────────────────

async def run_scheduler(bot: Bot) -> None:
    print("[content] планировщик запущен")
    while True:
        try:
            now = datetime.now(MSK)
            # Сброс списка постов нового дня
            posted_today.clear()

            schedule = get_daily_schedule()
            count = len(schedule)
            print(f"[content] сегодня {count} постов: {[f'{h}:{m:02d}' for h,m in schedule]}")

            for hour, minute in schedule:
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target <= now:
                    continue

                wait = (target - now).total_seconds()
                print(f"[content] следующий пост в {hour}:{minute:02d} МСК (через {wait/60:.0f} мин)")
                await asyncio.sleep(wait)

                post = await generate_post()
                if post:
                    await send_for_approval(bot, post)

                now = datetime.now(MSK)

            # Ждём до 09:00 следующего дня
            from datetime import timedelta
            tomorrow = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(days=1)
            wait = (tomorrow - datetime.now(MSK)).total_seconds()
            print(f"[content] все посты на сегодня готовы, следующий цикл через {wait/3600:.1f}ч")
            await asyncio.sleep(max(wait, 60))

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[content] scheduler error: {e}")
            await asyncio.sleep(300)

# ── Запуск ───────────────────────────────────────────────────────

async def run():
    bot = Bot(token=CONTENT_BOT_TOKEN)
    print("[content] бот @irioqwqhdqdiw12332_bot инициализирован")

    # Пишем в HQ
    try:
        now = datetime.now(MSK).strftime("%d.%m.%Y %H:%M")
        owner_token = os.getenv("TELEGRAM_BOT_TOKEN")
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(
                f"https://api.telegram.org/bot{owner_token}/sendMessage",
                json={
                    "chat_id": -1004385799918,
                    "text": f"📢 Контент-агент на линии · {now} МСК\nПостинг в @kroshide · 3-5 постов в день"
                }
            )
    except Exception as e:
        print(f"[content] hq notify error: {e}")

    asyncio.create_task(run_scheduler(bot))
    await asyncio.Event().wait()


def start_content_agent():
    import threading

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    print("[content] content_agent поток запущен")
