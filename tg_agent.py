import os
import json
import re
import httpx
from datetime import datetime
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import anthropic

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
conversations = {}      # chat_id -> list of messages
order_context = {}      # chat_id -> {"name": ..., "size": ..., "price": ...}


def _is_purchase(text: str) -> bool:
    return any(kw in text.lower() for kw in PURCHASE_KEYWORDS)


def _extract_price(text: str) -> int:
    """Ищем число перед ₽ или руб в тексте."""
    match = re.search(r"(\d[\d\s]*)\s*[₽р]", text)
    if match:
        return int(match.group(1).replace(" ", ""))
    return 0


def _update_order_context(chat_id: int, text: str, inventory: list) -> None:
    """Обновляем контекст заказа если в тексте упомянут товар из склада."""
    if chat_id not in order_context:
        order_context[chat_id] = {"name": "", "size": "", "price": 0}

    text_lower = text.lower()

    # Ищем товар из склада в тексте
    for item in inventory:
        item_name = item.get("name", "")
        if item_name.lower() in text_lower or any(
            word in text_lower for word in item_name.lower().split() if len(word) > 3
        ):
            order_context[chat_id]["name"] = item_name
            order_context[chat_id]["price"] = item.get("price", 0)
            break

    # Ищем размер (S, M, L, XL, XXL, XS, 3XL)
    size_match = re.search(r"\b(3XL|XXL|XL|XS|[SML])\b", text, re.IGNORECASE)
    if size_match:
        order_context[chat_id]["size"] = size_match.group(1).upper()

    # Ищем цену
    price = _extract_price(text)
    if price > 0:
        order_context[chat_id]["price"] = price


async def get_inventory() -> tuple[str, list]:
    """Возвращает (строку для промпта, список items)."""
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
            return "\n".join(lines), items
    except Exception:
        return "Склад временно недоступен", []


async def notify_sale(user_name: str, chat_id: int) -> dict:
    ctx = order_context.get(chat_id, {})
    payload = {
        "date": datetime.now().strftime("%d.%m"),
        "name": ctx.get("name", ""),
        "size": ctx.get("size", ""),
        "status": "Ожидает отправки",
        "price": ctx.get("price", 0),
        "cost": "",
        "article": "",
        "buyer": user_name,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            await http.post(N8N_ORDERS_URL, json=payload)
    except Exception as e:
        print(f"[sale] webhook error: {e}")
    return payload


async def build_system_prompt(inventory_text: str) -> str:
    return f"""Ты — менеджер по продажам одежды LOCAL Store. Отвечай покупателям вежливо, коротко и по делу.

ТОВАРЫ В НАЛИЧИИ (актуально):
{inventory_text}

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
    order_context[chat_id] = {"name": "", "size": "", "price": 0}
    await update.message.reply_text("👋 Привет! Я менеджер магазина LOCAL Store.\n\nЧем могу помочь? 😊")


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversations[chat_id] = []
    order_context[chat_id] = {"name": "", "size": "", "price": 0}
    await update.message.reply_text("🔄 Диалог сброшен. Начинаем заново!")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != OWNER_CHAT_ID:
        return
    total = sum(len(v) for v in conversations.values())
    await update.message.reply_text(
        f"📊 Статистика:\n• Активных диалогов: {len(conversations)}\n• Всего сообщений: {total}"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_text = update.message.text
    user_name = update.effective_user.first_name or "Покупатель"

    if chat_id not in conversations:
        conversations[chat_id] = []

    # Тянем склад и обновляем контекст заказа
    inventory_text, inventory_items = await get_inventory()
    _update_order_context(chat_id, user_text, inventory_items)

    conversations[chat_id].append({"role": "user", "content": f"{user_name}: {user_text}"})
    if len(conversations[chat_id]) > 20:
        conversations[chat_id] = conversations[chat_id][-20:]

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=await build_system_prompt(inventory_text),
            messages=conversations[chat_id]
        )
        reply = response.content[0].text

        # Обновляем контекст из ответа агента (там могут быть название и цена)
        _update_order_context(chat_id, reply, inventory_items)

        conversations[chat_id].append({"role": "assistant", "content": reply})

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

            status = "✅ Записано в таблицу" if order["name"] else "⚠️ Товар не определён — проверь таблицу"
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"🛍 Новый заказ!\n"
                     f"Покупатель: {user_name}\n"
                     f"Товар: {order['name'] or '?'}\n"
                     f"Размер: {order['size'] or '?'}\n"
                     f"Цена: {order['price'] or '?'} ₽\n"
                     f"{status}"
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
