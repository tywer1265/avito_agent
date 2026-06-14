@echo off
echo ========================================
echo   AVITO AGENTS - SETUP
echo ========================================

echo [1/4] Создаём виртуальное окружение...
python -m venv venv

echo [2/4] Активируем venv...
call venv\Scripts\activate

echo [3/4] Устанавливаем зависимости...
pip install aiohttp asyncpg orjson pydantic-core --pre
pip install structlog tenacity python-dotenv pydantic-settings apscheduler pytz python-dateutil prometheus-client python-telegram-bot httpx fastapi uvicorn sqlalchemy alembic anthropic

echo [4/4] Инициализируем базу данных...
python -c "import asyncio; from core.database import init_db; asyncio.run(init_db())"

echo ========================================
echo   ГОТОВО! Запускай: python main.py
echo ========================================
pause
