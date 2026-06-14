import asyncio
import os
import json
from datetime import datetime
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import anthropic

# ── Config ──────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
OWNER_CHAT_ID = int(os.getenv("TELEGRAM_OWNER_CHAT_ID", "5016220108"))

client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ── Память диалогов ─────────────────────────────────────
conversations = {}  # chat_id -> list of messages
products_db = {
    "худи": {"name": "Худи оверсайз", "price": 2900, "sizes": ["S", "M", "L", "XL"], "color": "чёрный/белый/серый"},
    "футболка": {"name": "Футболка базовая", "price": 1500, "sizes": ["S", "M", "L", "XL"], "color": "белый/чёрный"},
    "штаны": {"name": "Штаны карго", "price": 3200, "sizes": ["S", "M", "L", "XL"], "color": "чёрный/хаки"},
    "кепка": {"name": "Кепка", "price": 1200, "sizes": ["ONE SIZE"], "color": "чёрный/белый"},
}

SYSTEM_PROMPT = """Ты — менеджер по продажам одежды на Авито. Твоя задача — отвечать покупателям вежливо, коротко и по делу.

ТОВАРЫ В НАЛИЧИИ:
- Худи оверсайз: 2900₽, размеры S/M/L/XL, цвета: чёрный/белый/серый
- Футболка базовая: 1500₽, размеры S/M/L/XL, цвета: белый/чёрный  
- Штаны карго: 3200₽, размеры S/M/L/XL, цвета: чёрный/хаки
- Кепка: 1200₽, ONE SIZE, цвета: чёрный/белый

ПРАВИЛА:
1. Отвечай коротко — 1-3 предложения максимум
2. При торге можешь снизить цену МАКСИМУМ на 10%
3. Доставка: СДЭК 2-5 дней, Почта России 5-10 дней, самовывоз Москва
4. Если спрашивают про размер — уточни рост и вес покупателя
5. Всегда заканчивай вопросом или призывом к действию
6. Пиши как живой человек, без официоза
7. Если покупатель грубит — вежливо но твёрдо отвечай
8. При эскалации (возврат, конфликт) — напиши "ЭСКАЛАЦИЯ:" в начале ответа

СТИЛЬ: дружелюбный, живой, не роботизированный"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversations[chat_id] = []
    await update.message.reply_text(
        "👋 Привет! Я менеджер магазина LOCAL Store.\n\n"
        "У нас есть:\n"
        "• Худи оверсайз — 2900₽\n"
        "• Футболки — 1500₽\n"
        "• Штаны карго — 3200₽\n"
        "• Кепки — 1200₽\n\n"
        "Чем могу помочь? 😊"
    )

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversations[chat_id] = []
    await update.message.reply_text("🔄 Диалог сброшен. Начинаем заново!")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != OWNER_CHAT_ID:
        return
    total = sum(len(v) for v in conversations.values())
    chats = len(conversations)
    await update.message.reply_text(
        f"📊 Статистика:\n"
        f"• Активных диалогов: {chats}\n"
        f"• Всего сообщений: {total}"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_text = update.message.text
    user_name = update.effective_user.first_name or "Покупатель"
    
    # Инициализация диалога
    if chat_id not in conversations:
        conversations[chat_id] = []
    
    # Добавляем сообщение пользователя
    conversations[chat_id].append({
        "role": "user",
        "content": f"{user_name}: {user_text}"
    })
    
    # Ограничиваем историю — последние 20 сообщений
    if len(conversations[chat_id]) > 20:
        conversations[chat_id] = conversations[chat_id][-20:]
    
    # Показываем что печатаем
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    
    try:
        # Вызов Claude Haiku (быстрый и дешёвый)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=conversations[chat_id]
        )
        
        reply = response.content[0].text
        
        # Добавляем ответ в историю
        conversations[chat_id].append({
            "role": "assistant", 
            "content": reply
        })
        
        # Проверяем эскалацию
        if reply.startswith("ЭСКАЛАЦИЯ:"):
            clean_reply = reply.replace("ЭСКАЛАЦИЯ:", "").strip()
            await update.message.reply_text(clean_reply)
            # Уведомляем владельца
            if OWNER_CHAT_ID:
                await context.bot.send_message(
                    chat_id=OWNER_CHAT_ID,
                    text=f"🚨 ЭСКАЛАЦИЯ от {user_name}!\n\nСообщение: {user_text}\n\nОтвет агента: {clean_reply}"
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
    
    print("✅ Агент запущен! Пиши боту в Telegram.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
