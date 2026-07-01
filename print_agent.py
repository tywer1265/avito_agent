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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# ── ENV ──────────────────────────────────────────────────────────────────────
PRINT_BOT_TOKEN     = os.getenv("PRINT_BOT_TOKEN")
OWNER_CHAT_ID       = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
DATABASE_URL        = os.getenv("DATABASE_URL_ASYNCPG")
SHEETS_ID           = "1oE7xI_BwogoHCZ0On8rzjNlHCLUfquXMJgAvpM25rNI"
DRIVE_FOLDER_ID     = "1KHe5PmOTV-ql4FbdjSpKMXLX1z7cn1q-"
GOOGLE_SA_JSON      = os.getenv("GOOGLE_SA_JSON")  # JSON строка в env

# Размер листа: 57 x 100 см при 150 dpi (оптимум для генерации)
SHEET_W_CM  = 57
SHEET_H_CM  = 100
DPI         = 150
SHEET_W_PX  = int(SHEET_W_CM / 2.54 * DPI)
SHEET_H_PX  = int(SHEET_H_CM / 2.54 * DPI)

# ── GOOGLE AUTH ───────────────────────────────────────────────────────────────
def get_google_services():
    if GOOGLE_SA_JSON:
        info = json.loads(GOOGLE_SA_JSON)
    else:
        # fallback: читаем файл если есть локально
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
            id          SERIAL PRIMARY KEY,
            article     TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL,
            colors      TEXT,
            sizes       TEXT,
            drive_file_id TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS print_sheets (
            id          SERIAL PRIMARY KEY,
            sheet_date  DATE NOT NULL,
            status      TEXT DEFAULT 'open',
            tg_file_id  TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS print_queue (
            id          SERIAL PRIMARY KEY,
            order_id    TEXT NOT NULL,
            article     TEXT NOT NULL,
            sheet_id    INTEGER REFERENCES print_sheets(id),
            status      TEXT DEFAULT 'pending',
            added_at    TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(order_id, article)
        );
    """)
    await conn.close()
    logger.info("DB init done")

# ── GOOGLE DRIVE ──────────────────────────────────────────────────────────────
def find_drive_file(drive, article: str) -> str | None:
    """Ищет файл по артикулу в папке Prints. Возвращает file_id."""
    for ext in ["png", "jpg", "jpeg", "pdf"]:
        name = f"{article}.{ext}"
        result = drive.files().list(
            q=f"name='{name}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
            fields="files(id,name)"
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
    return None

def download_drive_file(drive, file_id: str) -> bytes:
    """Скачивает файл из Drive, возвращает байты."""
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def get_confirmed_orders(sheets) -> list[dict]:
    """
    Возвращает заказы со статусом 'Подтверждён' из листа 'Продажи'.
    Колонки: A=Заказ, B=Дата, C=Наименование, D=Размер, E=Статус, K=Артикул
    """
    result = sheets.spreadsheets().values().get(
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
        article = row[10].strip() if len(row) > 10 else ""
        order_id = row[0].strip() if row else ""
        if not article or not order_id:
            continue
        orders.append({
            "order_id": order_id,
            "name": row[2] if len(row) > 2 else "",
            "size": row[3] if len(row) > 3 else "",
            "article": article,
        })
    return orders

# ── BIN PACKING (простой greedy) ──────────────────────────────────────────────
def pack_images(images: list[Image.Image]) -> Image.Image:
    """
    Упаковывает изображения в лист 57x100 см максимально плотно.
    Простой алгоритм: строки сверху вниз, изображения масштабируются пропорционально.
    """
    sheet = Image.new("RGBA", (SHEET_W_PX, SHEET_H_PX), (255, 255, 255, 255))
    x, y = 0, 0
    row_h = 0

    for img in images:
        # Масштабируем если шире листа
        if img.width > SHEET_W_PX:
            ratio = SHEET_W_PX / img.width
            img = img.resize((SHEET_W_PX, int(img.height * ratio)), Image.LANCZOS)

        # Если не влезает в строку — новая строка
        if x + img.width > SHEET_W_PX:
            x = 0
            y += row_h
            row_h = 0

        # Если не влезает по высоте — стоп
        if y + img.height > SHEET_H_PX:
            logger.warning("Лист заполнен, остаток принтов не уместился")
            break

        sheet.paste(img, (x, y), img if img.mode == "RGBA" else None)
        x += img.width
        row_h = max(row_h, img.height)

    return sheet

def sheet_to_pdf(pil_image: Image.Image) -> bytes:
    """Конвертирует PIL Image в PDF байты нужного размера."""
    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=(SHEET_W_CM * cm, SHEET_H_CM * cm))

    # Сохраняем как PNG во временный файл
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        pil_image.save(tmp.name, "PNG")
        tmp_path = tmp.name

    pdf.drawImage(tmp_path, 0, 0, width=SHEET_W_CM * cm, height=SHEET_H_CM * cm)
    pdf.save()
    os.unlink(tmp_path)
    return buf.getvalue()

# ── CORE: BUILD SHEET ─────────────────────────────────────────────────────────
async def build_print_sheet() -> tuple[bytes | None, list[str], int | None]:
    """
    Собирает gang sheet из очереди.
    Возвращает (pdf_bytes, список артикулов, sheet_id) или (None, [], None)
    """
    conn = await get_db()
    drive, sheets_svc = get_google_services()

    try:
        # Получаем подтверждённые заказы из Google Sheets
        orders = get_confirmed_orders(sheets_svc)
        logger.info(f"Подтверждённых заказов: {len(orders)}")

        # Открытый лист на сегодня
        today = date.today()
        sheet_row = await conn.fetchrow(
            "SELECT id FROM print_sheets WHERE sheet_date=$1 AND status='open'", today
        )
        if not sheet_row:
            sheet_id = await conn.fetchval(
                "INSERT INTO print_sheets (sheet_date) VALUES ($1) RETURNING id", today
            )
        else:
            sheet_id = sheet_row["id"]

        # Фильтруем уже добавленные
        articles_to_print = []
        for order in orders:
            exists = await conn.fetchrow(
                "SELECT id FROM print_queue WHERE order_id=$1 AND article=$2",
                order["order_id"], order["article"]
            )
            if not exists:
                articles_to_print.append(order)

        if not articles_to_print:
            logger.info("Нет новых принтов для добавления")
            return None, [], sheet_id

        # Скачиваем изображения
        images = []
        added_articles = []
        for order in articles_to_print:
            file_id = find_drive_file(drive, order["article"])
            if not file_id:
                logger.warning(f"Файл не найден для артикула {order['article']}")
                continue

            file_bytes = download_drive_file(drive, file_id)
            try:
                img = Image.open(io.BytesIO(file_bytes)).convert("RGBA")
                images.append(img)
                added_articles.append(order["article"])

                # Записываем в очередь
                await conn.execute(
                    "INSERT INTO print_queue (order_id, article, sheet_id, status) VALUES ($1,$2,$3,'added') ON CONFLICT DO NOTHING",
                    order["order_id"], order["article"], sheet_id
                )
            except Exception as e:
                logger.error(f"Ошибка обработки изображения {order['article']}: {e}")

        if not images:
            return None, [], sheet_id

        # Компоновка
        sheet_img = pack_images(images)
        pdf_bytes = sheet_to_pdf(sheet_img)

        return pdf_bytes, added_articles, sheet_id

    finally:
        await conn.close()

# ── TELEGRAM BOT ──────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text(
        "🖨 *Print Agent онлайн*\n\n"
        "Команды:\n"
        "/status — текущая очередь\n"
        "/makesheet — собрать лист прямо сейчас\n"
        "/good — одобрить лист и закрыть\n"
        "/nostop — лист ещё не готов, продолжить накапливать\n"
        "/listprints — все принты в базе\n"
        "/addprint АРТИКУЛ НАЗВАНИЕ — добавить принт вручную",
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

    queue = await conn.fetch(
        "SELECT article, status FROM print_queue WHERE sheet_id=$1", sheet["id"]
    )
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
    await update.message.reply_text("⏳ Собираю лист...")
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

    await conn.execute(
        "UPDATE print_sheets SET status='approved' WHERE id=$1", sheet["id"]
    )
    await conn.execute(
        "UPDATE print_queue SET status='printed' WHERE sheet_id=$1", sheet["id"]
    )
    await conn.close()
    await update.message.reply_text("✅ Лист одобрен. Принты отмечены как напечатанные.\nСледующие заказы пойдут в новый лист.")

async def cmd_nostop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text(
        "⏸ Понял. Лист остаётся открытым.\n"
        "Новые подтверждённые заказы будут добавляться в него.\n"
        "Когда будет готов — напиши /good"
    )

async def cmd_listprints(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    conn = await get_db()
    prints = await conn.fetch("SELECT article, name, colors, sizes FROM prints ORDER BY article")
    await conn.close()

    if not prints:
        await update.message.reply_text("База принтов пуста.")
        return

    text = "🗂 *База принтов:*\n\n"
    for p in prints:
        text += f"• `{p['article']}` — {p['name']}"
        if p["colors"]:
            text += f" | {p['colors']}"
        text += "\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_addprint(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        return
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /addprint АРТИКУЛ НАЗВАНИЕ [ЦВЕТА]")
        return

    article = args[0]
    name = " ".join(args[1:])
    conn = await get_db()

    # Проверяем файл на Drive
    drive, _ = get_google_services()
    file_id = find_drive_file(drive, article)

    await conn.execute(
        "INSERT INTO prints (article, name, drive_file_id) VALUES ($1,$2,$3) ON CONFLICT (article) DO UPDATE SET name=$2, drive_file_id=$3",
        article, name, file_id
    )
    await conn.close()

    if file_id:
        await update.message.reply_text(f"✅ Принт `{article}` добавлен. Файл найден на Drive.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"⚠️ Принт `{article}` добавлен, но файл на Drive не найден. Загрузи `{article}.png` в папку Prints.", parse_mode="Markdown")

# ── SEND SHEET TO OWNER ───────────────────────────────────────────────────────
async def send_sheet_to_owner(bot):
    """Собирает gang sheet и отправляет владельцу на одобрение."""
    try:
        pdf_bytes, articles, sheet_id = await build_print_sheet()
    except Exception as e:
        logger.error(f"Ошибка сборки листа: {e}")
        await bot.send_message(OWNER_CHAT_ID, f"❌ Ошибка при сборке листа: {e}")
        return

    if not pdf_bytes:
        await bot.send_message(OWNER_CHAT_ID, "ℹ️ Нет новых принтов для печати.")
        return

    articles_text = "\n".join(f"• {a}" for a in articles)
    caption = (
        f"🖨 *Лист на печать* #{sheet_id}\n\n"
        f"Принтов: {len(articles)}\n{articles_text}\n\n"
        f"Одобри командой /good\n"
        f"Если лист не готов — /nostop (добавим ещё)"
    )

    pdf_file = io.BytesIO(pdf_bytes)
    pdf_file.name = f"print_sheet_{date.today()}.pdf"

    await bot.send_document(
        OWNER_CHAT_ID,
        document=pdf_file,
        caption=caption,
        parse_mode="Markdown"
    )

# ── SCHEDULER: 12:00 МСК ─────────────────────────────────────────────────────
async def daily_sheet_job(bot):
    """Запускается каждый день в 12:00 МСК."""
    while True:
        now = datetime.now(MSK)
        # Следующий запуск в 12:00
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target.replace(day=target.day + 1)
        wait_seconds = (target - now).total_seconds()
        logger.info(f"Следующий запуск листа через {wait_seconds/3600:.1f} ч")
        await asyncio.sleep(wait_seconds)
        logger.info("12:00 МСК — собираю print sheet")
        await send_sheet_to_owner(bot)

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    await init_db()

    app = Application.builder().token(PRINT_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("makesheet",  cmd_makesheet))
    app.add_handler(CommandHandler("good",       cmd_good))
    app.add_handler(CommandHandler("nostop",     cmd_nostop))
    app.add_handler(CommandHandler("listprints", cmd_listprints))
    app.add_handler(CommandHandler("addprint",   cmd_addprint))

    # Запускаем планировщик параллельно
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        # Выпадающее меню команд в Telegram
        from telegram import BotCommand
        await app.bot.set_my_commands([
            BotCommand("start",      "Запустить агента"),
            BotCommand("makesheet",  "Собрать лист на печать сейчас"),
            BotCommand("good",       "Одобрить лист — отправить в печать"),
            BotCommand("nostop",     "Лист не готов — продолжить накапливать"),
            BotCommand("status",     "Текущая очередь принтов"),
            BotCommand("listprints", "База всех принтов"),
            BotCommand("addprint",   "Добавить принт: /addprint АРТИКУЛ НАЗВАНИЕ"),
        ])

        # Стартовое сообщение
        await app.bot.send_message(
            OWNER_CHAT_ID,
            "🖨 *Print Agent запущен*\n\n"
            "Слежу за заказами со статусом «Подтверждён».\n"
            "Каждый день в 12:00 МСК собираю лист на печать.",
            parse_mode="Markdown"
        )

        # Планировщик
        asyncio.create_task(daily_sheet_job(app.bot))

        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
