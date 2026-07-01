import os
import io
import json
import logging
import asyncio
import tempfile
from datetime import datetime, date
from zoneinfo import ZoneInfo
import httpx
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("print_agent")

MSK = ZoneInfo("Europe/Moscow")

# ── ENV ───────────────────────────────────────────────────────────────────────
PRINT_BOT_TOKEN  = os.getenv("PRINT_BOT_TOKEN")
OWNER_CHAT_ID    = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
DATABASE_URL     = os.getenv("DATABASE_URL_ASYNCPG")
SHEETS_ID        = "1oE7xI_BwogoHCZ0On8rzjNlHCLUfquXMJgAvpM25rNI"
DRIVE_FOLDER_ID  = "1KHe5PmOTV-ql4FbdjSpKMXLX1z7cn1q-"
GOOGLE_SA_JSON   = os.getenv("GOOGLE_SA_JSON")

SHEET_W_CM = 57
SHEET_H_CM = 100
DPI        = 300  # 300 dpi — стандарт печати, Photoshop открывать при 300 ppi
SHEET_W_PX = int(SHEET_W_CM / 2.54 * DPI)
SHEET_H_PX = int(SHEET_H_CM / 2.54 * DPI)

# ── SESSION STATE (in-memory) ─────────────────────────────────────────────────
# Хранит состояние интерактивных сессий
# { chat_id: { "mode": "addtosheet"|"newfile", "articles": [], "widths": {} } }
SESSION: dict = {}

# ── GOOGLE AUTH ───────────────────────────────────────────────────────────────
def get_google_services():
    if GOOGLE_SA_JSON:
        info = json.loads(GOOGLE_SA_JSON)
    else:
        with open("kroshide-4ee49a8d1bd5.json") as f:
            info = json.load(f)
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    drive  = build("drive",  "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets

# ── DATABASE ──────────────────────────────────────────────────────────────────
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

async def init_db():
    conn = await get_db()
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS prints (
            id            SERIAL PRIMARY KEY,
            article       TEXT UNIQUE NOT NULL,
            name          TEXT NOT NULL,
            colors        TEXT,
            sizes         TEXT,
            width_cm      FLOAT,
            drive_file_id TEXT,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS print_sheets (
            id         SERIAL PRIMARY KEY,
            sheet_date DATE NOT NULL,
            status     TEXT DEFAULT 'open',
            tg_file_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS print_queue (
            id       SERIAL PRIMARY KEY,
            order_id TEXT NOT NULL,
            article  TEXT NOT NULL,
            sheet_id INTEGER REFERENCES print_sheets(id),
            status   TEXT DEFAULT 'pending',
            added_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(order_id, article)
        );
    """)
    # Миграция: добавляем width_cm если нет
    await conn.execute("ALTER TABLE prints ADD COLUMN IF NOT EXISTS width_cm FLOAT")
    await conn.close()
    logger.info("DB init done")

# ── GOOGLE DRIVE ──────────────────────────────────────────────────────────────
def find_drive_file(drive, article: str) -> str | None:
    for ext in ["png", "jpg", "jpeg", "pdf"]:
        result = drive.files().list(
            q=f"name='{article}.{ext}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
            fields="files(id,name)"
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
    return None

def download_drive_file(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def get_confirmed_orders(sheets_svc) -> list[dict]:
    result = sheets_svc.spreadsheets().values().get(
        spreadsheetId=SHEETS_ID,
        range="Продажи!A2:K"
    ).execute()
    rows = result.get("values", [])
    orders = []
    for row in rows:
        if len(row) < 5:
            continue
        status = row[4].strip() if len(row) > 4 else ""
        if status.replace("ё", "е") != "Подтвержден":
            continue
        article  = row[10].strip() if len(row) > 10 else ""
        order_id = row[0].strip() if row else ""
        if not article or not order_id:
            continue
        orders.append({
            "order_id": order_id,
            "name":     row[2] if len(row) > 2 else "",
            "size":     row[3] if len(row) > 3 else "",
            "article":  article,
        })
    return orders

# ── IMAGE HELPERS ─────────────────────────────────────────────────────────────
def cm_to_px(width_cm: float) -> int:
    """Конвертирует см в пиксели через пропорцию листа — точно независимо от DPI."""
    px = int(SHEET_W_PX * width_cm / SHEET_W_CM)
    logger.info(f"cm_to_px: {width_cm} см → {px} px из {SHEET_W_PX} (лист {SHEET_W_CM} см)")
    return px

def resize_to_width(img: Image.Image, target_px: int) -> Image.Image:
    ratio = target_px / img.width
    return img.resize((target_px, int(img.height * ratio)), Image.LANCZOS)

def pack_images(images_with_widths: list[tuple[Image.Image, int | None]]) -> Image.Image:
    """
    Компонует принты в лист 57x100 см плотно, сверху вниз.
    images_with_widths: [(img, target_width_px or None), ...]
    Прозрачный фон.
    """
    sheet  = Image.new("RGBA", (SHEET_W_PX, SHEET_H_PX), (0, 0, 0, 0))
    x, y   = 0, 0
    row_h  = 0

    for img, target_w in images_with_widths:
        img = img.convert("RGBA")

        if target_w:
            final_w = min(target_w, SHEET_W_PX)
            logger.info(f"pack: target_w={target_w}px → final={final_w}px = {final_w/SHEET_W_PX*SHEET_W_CM:.1f} см")
            img = resize_to_width(img, final_w)
        else:
            default_w = SHEET_W_PX // 3
            logger.info(f"pack: no target_w, дефолт={default_w}px = {default_w/SHEET_W_PX*SHEET_W_CM:.1f} см")
            img = resize_to_width(img, default_w)

        # Если не влезает в строку — новая строка
        if x + img.width > SHEET_W_PX:
            x = 0
            y += row_h
            row_h = 0

        # Если не влезает по высоте — стоп
        if y + img.height > SHEET_H_PX:
            logger.warning("Лист заполнен, остаток не уместился")
            break

        sheet.paste(img, (x, y), img)
        x    += img.width
        row_h = max(row_h, img.height)

    return sheet

def sheet_to_pdf(pil_image: Image.Image) -> bytes:
    """Конвертирует PIL Image в PDF. Прозрачный фон через PNG байты."""
    # Сохраняем PNG во временный файл (reportlab требует путь или ImageReader)
    from reportlab.lib.utils import ImageReader
    png_buf = io.BytesIO()
    pil_image.save(png_buf, "PNG")
    png_buf.seek(0)
    img_reader = ImageReader(png_buf)

    pdf_buf = io.BytesIO()
    pdf = canvas.Canvas(pdf_buf, pagesize=(SHEET_W_CM * cm, SHEET_H_CM * cm))
    pdf.drawImage(img_reader, 0, 0, width=SHEET_W_CM * cm, height=SHEET_H_CM * cm, mask="auto")
    pdf.save()
    return pdf_buf.getvalue()

# ── CORE: BUILD SHEET FROM ORDERS ─────────────────────────────────────────────
async def build_print_sheet() -> tuple[bytes | None, list[str], int | None]:
    conn = await get_db()
    drive, sheets_svc = get_google_services()
    try:
        orders = get_confirmed_orders(sheets_svc)
        logger.info(f"Подтверждённых заказов: {len(orders)}")

        today = date.today()
        sheet_row = await conn.fetchrow(
            "SELECT id FROM print_sheets WHERE sheet_date=$1 AND status='open'", today
        )
        sheet_id = sheet_row["id"] if sheet_row else await conn.fetchval(
            "INSERT INTO print_sheets (sheet_date) VALUES ($1) RETURNING id", today
        )

        articles_to_print = []
        for order in orders:
            exists = await conn.fetchrow(
                "SELECT id FROM print_queue WHERE order_id=$1 AND article=$2",
                order["order_id"], order["article"]
            )
            if not exists:
                articles_to_print.append(order)

        if not articles_to_print:
            return None, [], sheet_id

        images_with_widths = []
        added_articles = []
        for order in articles_to_print:
            file_id = find_drive_file(drive, order["article"])
            if not file_id:
                logger.warning(f"Файл не найден: {order['article']}")
                continue
            file_bytes = download_drive_file(drive, file_id)
            try:
                img = Image.open(io.BytesIO(file_bytes)).convert("RGBA")
                # Берём ширину из базы принтов если есть
                p = await conn.fetchrow("SELECT width_cm FROM prints WHERE article=$1", order["article"])
                target_w = cm_to_px(p["width_cm"]) if p and p["width_cm"] else None
                images_with_widths.append((img, target_w))
                added_articles.append(order["article"])
                await conn.execute(
                    "INSERT INTO print_queue (order_id, article, sheet_id, status) VALUES ($1,$2,$3,'added') ON CONFLICT DO NOTHING",
                    order["order_id"], order["article"], sheet_id
                )
            except Exception as e:
                logger.error(f"Ошибка изображения {order['article']}: {e}")

        if not images_with_widths:
            return None, [], sheet_id

        def render():
            sheet_img = pack_images(images_with_widths)
            return sheet_to_pdf(sheet_img)
        logger.info(f"Запускаю pack+pdf в to_thread для {len(images_with_widths)} принтов")
        pdf_bytes = await asyncio.to_thread(render)
        logger.info(f"PDF готов: {len(pdf_bytes)} байт")
        return pdf_bytes, added_articles, sheet_id
    finally:
        await conn.close()

# ── CORE: BUILD SHEET FROM ARTICLE LIST ───────────────────────────────────────
async def build_sheet_from_articles(articles: list[str], custom_widths: dict[str, float]) -> tuple[bytes | None, list[str]]:
    """Собирает PDF из произвольного списка артикулов."""
    conn = await get_db()
    drive, _ = get_google_services()
    try:
        images_with_widths = []
        found = []
        for article in articles:
            file_id = find_drive_file(drive, article)
            if not file_id:
                logger.warning(f"Файл не найден: {article}")
                continue
            file_bytes = download_drive_file(drive, file_id)
            try:
                img = Image.open(io.BytesIO(file_bytes)).convert("RGBA")
                if article in custom_widths:
                    target_w = cm_to_px(custom_widths[article])
                else:
                    p = await conn.fetchrow("SELECT width_cm FROM prints WHERE article=$1", article)
                    target_w = cm_to_px(p["width_cm"]) if p and p["width_cm"] else None
                images_with_widths.append((img, target_w))
                found.append(article)
            except Exception as e:
                logger.error(f"Ошибка {article}: {e}", exc_info=True)

        if not images_with_widths:
            return None, []

        # CPU-тяжёлые операции выносим в отдельный поток
        logger.info(f"Запускаю pack+pdf в to_thread для {len(images_with_widths)} принтов")
        def render():
            sheet_img = pack_images(images_with_widths)
            return sheet_to_pdf(sheet_img)

        pdf_bytes = await asyncio.to_thread(render)
        logger.info(f"PDF готов: {len(pdf_bytes)} байт")
        return pdf_bytes, found
    finally:
        await conn.close()

# ── TELEGRAM COMMANDS ──────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text(
        "🖨 *Print Agent онлайн*\n\n"
        "*/makesheet* — собрать лист из подтверждённых заказов\n"
        "*/addtosheet* — добавить принты в текущий лист вручную\n"
        "*/addnewfile* — создать новый файл из любых принтов\n"
        "*/good* — одобрить лист\n"
        "*/nostop* — лист не готов, накапливать дальше\n"
        "*/status* — очередь\n"
        "*/listprints* — база принтов\n"
        "*/addprint АРТИКУЛ НАЗВАНИЕ ШИРИНА\\_СМ* — добавить принт в базу\n"
        "*/resetqueue* — сбросить очередь",
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    conn = await get_db()
    today = date.today()
    sheet = await conn.fetchrow(
        "SELECT id, status FROM print_sheets WHERE sheet_date=$1 ORDER BY id DESC LIMIT 1", today
    )
    if not sheet:
        await update.message.reply_text("Сегодня листов нет.")
        await conn.close()
        return
    queue = await conn.fetch("SELECT article, status FROM print_queue WHERE sheet_id=$1", sheet["id"])
    await conn.close()
    text = f"📋 Лист #{sheet['id']} — статус: *{sheet['status']}*\n\n"
    for q in queue:
        icon = "✅" if q["status"] == "printed" else "🔄"
        text += f"{icon} {q['article']}\n"
    if not queue:
        text += "_Очередь пуста_"
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_makesheet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("⏳ Собираю лист из подтверждённых заказов...")
    await send_sheet_to_owner(ctx.bot)

async def cmd_good(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    conn = await get_db()
    today = date.today()
    sheet = await conn.fetchrow(
        "SELECT id FROM print_sheets WHERE sheet_date=$1 AND status='open' ORDER BY id DESC LIMIT 1", today
    )
    if not sheet:
        await update.message.reply_text("Нет открытого листа.")
        await conn.close()
        return
    await conn.execute("UPDATE print_sheets SET status='approved' WHERE id=$1", sheet["id"])
    await conn.execute("UPDATE print_queue SET status='printed' WHERE sheet_id=$1", sheet["id"])
    await conn.close()
    await update.message.reply_text("✅ Лист одобрен. Следующие заказы пойдут в новый лист.")

async def cmd_nostop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text(
        "⏸ Лист остаётся открытым.\nНовые заказы добавляются в него.\nКогда готов — /good"
    )

async def cmd_listprints(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    conn = await get_db()
    prints = await conn.fetch("SELECT article, name, width_cm FROM prints ORDER BY article")
    await conn.close()
    if not prints:
        await update.message.reply_text("База принтов пуста.")
        return
    text = "🗂 *База принтов:*\n\n"
    for p in prints:
        text += f"• `{p['article']}` — {p['name']}"
        if p["width_cm"]:
            text += f" | {p['width_cm']} см"
        text += "\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_addprint(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Добавить принт в базу: /addprint АРТИКУЛ НАЗВАНИЕ ШИРИНА_СМ"""
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /addprint АРТИКУЛ НАЗВАНИЕ ШИРИНА_СМ\nПример: /addprint a130 Bape Shark 25")
        return

    article = args[0]
    # Последний аргумент — ширина если число, иначе часть названия
    width_cm = None
    try:
        width_cm = float(args[-1])
        name = " ".join(args[1:-1])
    except ValueError:
        name = " ".join(args[1:])

    drive, _ = get_google_services()
    file_id = find_drive_file(drive, article)

    conn = await get_db()
    await conn.execute(
        """INSERT INTO prints (article, name, width_cm, drive_file_id)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (article) DO UPDATE SET name=$2, width_cm=$3, drive_file_id=$4""",
        article, name, width_cm, file_id
    )
    await conn.close()

    msg = f"✅ Принт `{article}` — {name}"
    if width_cm:
        msg += f" | {width_cm} см"
    msg += "\n"
    msg += "Файл найден на Drive ✅" if file_id else f"⚠️ Файл не найден. Загрузи `{article}.png` в папку Prints."
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_resetqueue(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    conn = await get_db()
    today = date.today()
    sheet = await conn.fetchrow(
        "SELECT id FROM print_sheets WHERE sheet_date=$1 AND status='open' ORDER BY id DESC LIMIT 1", today
    )
    if sheet:
        deleted = await conn.fetchval(
            "DELETE FROM print_queue WHERE sheet_id=$1 RETURNING count(*)", sheet["id"]
        )
        await conn.execute("DELETE FROM print_sheets WHERE id=$1", sheet["id"])
        await conn.close()
        await update.message.reply_text(
            f"🗑 Очередь сброшена. Удалено: {deleted or 0}.\nТеперь /makesheet соберёт лист заново."
        )
    else:
        await conn.close()
        await update.message.reply_text("Нет открытого листа для сброса.")

# ── ИНТЕРАКТИВ: /addtosheet ───────────────────────────────────────────────────
async def cmd_addtosheet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Добавить принты в текущий открытый лист вручную."""
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    SESSION[OWNER_CHAT_ID] = {"mode": "addtosheet", "articles": [], "widths": {}}
    await update.message.reply_text(
        "📋 *Режим: добавить в текущий лист*\n\n"
        "Пиши артикулы по одному.\n"
        "Если нужна конкретная ширина: `a130 25` (артикул пробел ширина в см)\n"
        "Когда закончишь — /done",
        parse_mode="Markdown"
    )

# ── ИНТЕРАКТИВ: /addnewfile ───────────────────────────────────────────────────
async def cmd_addnewfile(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Создать новый файл из произвольных принтов."""
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    SESSION[OWNER_CHAT_ID] = {"mode": "newfile", "articles": [], "widths": {}}
    await update.message.reply_text(
        "🆕 *Режим: новый файл*\n\n"
        "Пиши артикулы по одному.\n"
        "Если нужна конкретная ширина: `a130 25` (артикул пробел ширина в см)\n"
        "Когда закончишь — /done",
        parse_mode="Markdown"
    )

# ── /done — завершить сессию ──────────────────────────────────────────────────
async def cmd_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    session = SESSION.get(OWNER_CHAT_ID)
    if not session:
        await update.message.reply_text("Нет активной сессии. Начни с /addtosheet или /addnewfile")
        return

    articles = session["articles"]
    widths   = session["widths"]
    mode     = session["mode"]
    SESSION.pop(OWNER_CHAT_ID, None)

    if not articles:
        await update.message.reply_text("Ты не добавил ни одного артикула.")
        return

    await update.message.reply_text(f"⏳ Собираю файл из {len(articles)} принтов...")

    if mode == "addtosheet":
        # Добавляем в текущий открытый лист
        conn = await get_db()
        today = date.today()
        sheet_row = await conn.fetchrow(
            "SELECT id FROM print_sheets WHERE sheet_date=$1 AND status='open'", today
        )
        sheet_id = sheet_row["id"] if sheet_row else await conn.fetchval(
            "INSERT INTO print_sheets (sheet_date) VALUES ($1) RETURNING id", today
        )
        # Записываем в очередь с fake order_id = "manual_АРТИКУЛ"
        for art in articles:
            await conn.execute(
                "INSERT INTO print_queue (order_id, article, sheet_id, status) VALUES ($1,$2,$3,'added') ON CONFLICT DO NOTHING",
                f"manual_{art}", art, sheet_id
            )
        await conn.close()
        # Строим лист из всей очереди
        await send_sheet_to_owner(ctx.bot)

    elif mode == "newfile":
        try:
            pdf_bytes, found = await build_sheet_from_articles(articles, widths)
        except Exception as e:
            logger.error(f"Ошибка генерации PDF: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка генерации: {e}")
            return
        if not pdf_bytes:
            await update.message.reply_text("❌ Ни один файл не найден на Drive.")
            return
        not_found = [a for a in articles if a not in found]
        caption = f"🖨 *Новый файл*\n\nПринтов: {len(found)}\n" + "\n".join(f"• {a}" for a in found)
        if not_found:
            caption += f"\n\n⚠️ Не найдено: {', '.join(not_found)}"
        pdf_file = io.BytesIO(pdf_bytes)
        pdf_file.name = f"custom_sheet_{date.today()}.pdf"
        await ctx.bot.send_document(OWNER_CHAT_ID, document=pdf_file, caption=caption, parse_mode="Markdown")

# ── ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ (сессия) ──────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    session = SESSION.get(OWNER_CHAT_ID)
    if not session:
        return  # не в сессии — игнорируем

    text = update.message.text.strip()
    if not text:
        return

    parts = text.split()
    article = parts[0].lower()
    width_cm = None
    if len(parts) >= 2:
        try:
            width_cm = float(parts[1])
        except ValueError:
            pass

    session["articles"].append(article)
    if width_cm:
        session["widths"][article] = width_cm

    msg = f"➕ `{article}`"
    if width_cm:
        msg += f" — {width_cm} см"
    msg += f"\nВсего: {len(session['articles'])}. Пиши ещё или /done"
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── SEND SHEET TO OWNER ───────────────────────────────────────────────────────
async def send_sheet_to_owner(bot):
    try:
        pdf_bytes, articles, sheet_id = await build_print_sheet()
    except Exception as e:
        logger.error(f"Ошибка сборки листа: {e}")
        await bot.send_message(OWNER_CHAT_ID, f"❌ Ошибка: {e}")
        return

    if not pdf_bytes:
        await bot.send_message(OWNER_CHAT_ID, "ℹ️ Нет новых принтов для печати.")
        return

    articles_text = "\n".join(f"• {a}" for a in articles)
    caption = (
        f"🖨 *Лист на печать* #{sheet_id}\n\n"
        f"Принтов: {len(articles)}\n{articles_text}\n\n"
        f"/good — одобрить\n/nostop — лист не готов, накапливать дальше"
    )
    pdf_file = io.BytesIO(pdf_bytes)
    pdf_file.name = f"print_sheet_{date.today()}.pdf"
    await bot.send_document(OWNER_CHAT_ID, document=pdf_file, caption=caption, parse_mode="Markdown")

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def daily_sheet_job(bot):
    while True:
        now    = datetime.now(MSK)
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            from datetime import timedelta
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        logger.info(f"Следующий запуск листа через {wait/3600:.1f} ч")
        await asyncio.sleep(wait)
        logger.info("12:00 МСК — собираю print sheet")
        await send_sheet_to_owner(bot)

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    await init_db()

    app = Application.builder().token(PRINT_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("makesheet",   cmd_makesheet))
    app.add_handler(CommandHandler("good",        cmd_good))
    app.add_handler(CommandHandler("nostop",      cmd_nostop))
    app.add_handler(CommandHandler("listprints",  cmd_listprints))
    app.add_handler(CommandHandler("addprint",    cmd_addprint))
    app.add_handler(CommandHandler("resetqueue",  cmd_resetqueue))
    app.add_handler(CommandHandler("addtosheet",  cmd_addtosheet))
    app.add_handler(CommandHandler("addnewfile",  cmd_addnewfile))
    app.add_handler(CommandHandler("done",        cmd_done))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        await app.bot.set_my_commands([
            BotCommand("start",      "Справка по командам"),
            BotCommand("makesheet",  "Собрать лист из подтверждённых заказов"),
            BotCommand("addtosheet", "Добавить принты в текущий лист вручную"),
            BotCommand("addnewfile", "Создать новый файл из любых принтов"),
            BotCommand("good",       "Одобрить лист — отправить в печать"),
            BotCommand("nostop",     "Лист не готов — продолжить накапливать"),
            BotCommand("done",       "Завершить ввод артикулов"),
            BotCommand("status",     "Текущая очередь принтов"),
            BotCommand("listprints", "База всех принтов"),
            BotCommand("addprint",   "Добавить принт: АРТИКУЛ НАЗВАНИЕ ШИРИНА_СМ"),
            BotCommand("resetqueue", "Сбросить очередь"),
        ])

        await app.bot.send_message(
            OWNER_CHAT_ID,
            "🖨 *Print Agent запущен*\n\n"
            "Слежу за заказами со статусом «Подтверждён».\n"
            "Каждый день в 12:00 МСК собираю лист на печать.",
            parse_mode="Markdown"
        )

        asyncio.create_task(daily_sheet_job(app.bot))
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
