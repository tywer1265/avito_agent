import asyncio
import os
import json
import httpx
from datetime import datetime
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import anthropic

# ── Config ──────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("CLIENT_BOT_TOKEN")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))
N8N_INVENTORY_URL = "https://tywer1265.app.n8n.cloud/webhook/inventory"
N8N_ORDERS_URL = "https://tywer1265.app.n8n.cloud/webhook/orders/new"

PURCHASE_KEYWORDS = [
    "беру", "покупаю", "оформляю", "оформи", "давайте оформим",
    "договорились", "согласен", "буду брать", "хочу купить",
    "оплачу", "оплатил", "как оплатить", "куда переводить",
    "добавь в заказ", "оформляем", "подходит",
]

client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
conversations = {}


def _is_purchase(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in PURCHASE_KEYWORDS)


async def get_inventory() -> str:
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            resp = await http.get(N8N_INVENTORY_URL)
            data = resp.json()
            items = data.get("inventory", [])
            if not items:
                return "Склад временно недоступен"
            lines = []
            for item in items:
                lines.append(
                    f"- {item['name']} ({item['size']}): {item['price']}₽, остаток: {item['stock']} шт"
                )
            return "\n".join(lines)
    except Exception:
        return "Склад временно недоступен"


async def extract_order_from_dialog(history: list) -> dict:
    """Вытаскиваем товар, размер и цену из истории диалога через Haiku."""
    dialog_text = "\n".join(
        f"{m['role']}: {m['content']}" for m in history[-10:]
    )
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system="Ты извлекаешь данные заказа из диалога. Отвечай ТОЛЬКО валидным JSON без markdown.",
            messages=[{
                "role": "user",
                "content": f"""Из этого диалога извлеки данные последнего заказа:

{dialog_text}

Верни JSON:
{{"name": "название товара", "size": "размер", "price": 0}}

Если не можешь определить — оставь пустую строку или 0."""
            }]
        )
        raw = response.content[0].text.strip()
        return json.loads(raw)
    except Exception:
        return {"name": "", "size": "", "price": 0}


async def notify_sale(user_name: str, chat_id: int, history: list) -> dict:
    """Вытаскиваем данные из диалога и пишем в Sheets через n8n."""
    order = await extract_order_from_dialog(history)

    payload = {
        "date": datetime.now().strftime("%d.%m"),
        "name": order.get("name", ""),
        "size": order.get("size", ""),
        "status": "Ожидает отправки",
        "price": order.get("price", 0),
        "cost": "",
        "article": "",
        "buyer": user_name,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.post(N8N_ORDERS_URL, json=payload)
            if resp.status_code == 200:
                print(f"[sale] записан заказ: {payload['name']} {payload['size']} {payload['price']}₽")
    except Exception as e:
        print(f"[sale] ошибка webhook: {e}")

    return payload


async def build_system_prompt() -> str:
    inventory = await get_inventory()
    return f"""Ты — менеджер по продажам одежды LOCAL Store. Отвечай покупателям вежливо, коротко и по делу.

ТОВАРЫ В НАЛИЧИИ (актуально):
{inventory}

ПРАВИЛА:
1. Отвечай коротко — 1-3 предложения максимум
2. При торге можешь снизить цену МАКСИМУМ на 10%
3. Доставка: СДЭК 2-5 дней, Почта России 5-10 дней, самовывоз Москва
4. Если спрашивают про размер — уточни рост и вес покупателя
5. Всегда заканчивай вопросом или призывом к действию
6. Пиши как живой человек, без официоза
7. Если покупатель грубит — вежливо но твёрдо отвечай
8. При эскалации (возврат, конфликт) — напиши "ЭСКАЛАЦИЯ:" в начале ответа
9. Если товара нет в списке — честно скажи что нет в наличии
10. Когда покупатель подтверждает покупку — напиши "ПОКУПКА:" в начале ответа, затем уточни детали доставки

СТИЛЬ: дружелюбный, живой, не роботизированный"""


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversations[chat_id] = []
    await update.message.reply_text(
        "👋 Привет! Я менеджер магазина LOCAL Store.\n\nЧем могу помочь? 😊"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversations[chat_id] = []
    await update.message.reply_text("🔄 Диалог сброшен. Начинаем заново!")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != OWNER_CHAT_ID:
        return
    total = sum(len(v) for v in conversations.values())
    await update.message.reply_text(
        f"📊 Статистика:\n"
        f"• Активных диалогов: {len(conversations)}\n"
        f"• Всего сообщений: {total}"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_text = update.message.text
    user_name = update.effective_user.first_name or "Покупатель"

    if chat_id not in conversations:
        conversations[chat_id] = []

    conversations[chat_id].append({
        "role": "user",
        "content": f"{user_name}: {user_text}"
    })

    if len(conversations[chat_id]) > 20:
        conversations[chat_id] = conversations[chat_id][-20:]

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        system_prompt = await build_system_prompt()

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=system_prompt,
            messages=conversations[chat_id]
        )

        reply = response.content[0].text

        conversations[chat_id].append({
            "role": "assistant",
            "content": reply
        })

        is_sale = _is_purchase(user_text) or reply.startswith("ПОКУПКА:")

        if reply.startswith("ЭСКАЛАЦИЯ:"):
            clean_reply = reply.replace("ЭСКАЛАЦИЯ:", "").strip()
            await update.message.reply_text(clean_reply)
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🚨 ЭСКАЛАЦИЯ от {user_name}!\n\nСообщение: {user_text}\n\nОтвет: {clean_reply}"
            )

        elif is_sale:
            clean_reply = reply.replace("ПОКУПКА:", "").strip()
            await update.message.reply_text(clean_reply)

            # Вытаскиваем данные из диалога и пишем в Sheets
            order = await notify_sale(user_name, chat_id, conversations[chat_id])

            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🛍 Новый заказ!\n"
                     f"Покупатель: {user_name}\n"
                     f"Товар: {order['name'] or '?'}\n"
                     f"Размер: {order['size'] or '?'}\n"
                     f"Цена: {order['price'] or '?'} ₽\n"
                     f"{'⚠️ Проверь таблицу — данные могут быть неполными' if not order['name'] else '✅ Записано в таблицу'}"
            )
        else:
            await update.message.reply_text(reply)

    except Exception as e:
        await update.message.reply_text("Секунду, уточню информацию и отвечу! 🙏")
        print(f"Error: {e}")


def main():
    print("🤖 Запуск Telegram агента...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("✅ Агент запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
