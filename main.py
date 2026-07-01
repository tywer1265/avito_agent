# main.py
"""
Avito Agents — Main Entry Point
Starts FastAPI server + APScheduler + all agent schedules.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Header, status
from fastapi.responses import JSONResponse

from core.config import settings
from core.database import close_db, init_db
from core.scheduler import get_scheduler, setup_schedules
from core.telegram import send_alert

# ── Structured logging setup ──────────────────────────────────

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer() if not settings.is_production
        else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, settings.log_level.upper(), logging.INFO)
    ),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger("main")


# ── App lifespan ───────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    log.info("avito_agents.startup", env=settings.app_env)

    # 1. Init database tables
    await init_db()
    log.info("avito_agents.db_ready")

    # 2. Start scheduler
    scheduler = get_scheduler()
    setup_schedules(scheduler)
    scheduler.start()
    log.info("avito_agents.scheduler_started", jobs=len(scheduler.get_jobs()))

    # 3. Notify owner — убрано, стартовое сообщение отправляет tg_agent.py
    await asyncio.sleep(2)  # wait for bot thread

    yield  # ← app runs here

    # Shutdown
    log.info("avito_agents.shutdown")
    scheduler.shutdown(wait=False)
    await close_db()


# ── FastAPI app ────────────────────────────────────────────────

app = FastAPI(
    title="Avito Agents",
    description="Multi-agent automation system for Avito clothing business",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if not settings.is_production else None,
    redoc_url=None,
)


# ── Auth helper ───────────────────────────────────────────────

def _verify_token(x_api_key: str = Header(None)) -> None:
    if x_api_key != settings.secret_key:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


# ── Health & status endpoints ──────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.app_env}


@app.get("/status")
async def system_status(x_api_key: str = Header(None)):
    _verify_token(x_api_key)
    scheduler = get_scheduler()
    jobs = [
        {
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
        }
        for job in scheduler.get_jobs()
    ]
    return {"scheduler_jobs": jobs, "total": len(jobs)}


# ── Manual agent trigger endpoints ────────────────────────────

@app.post("/trigger/{agent_name}")
async def trigger_agent(
    agent_name: str,
    payload: dict = None,
    x_api_key: str = Header(None),
):
    """Manually trigger any agent. Useful for testing and one-off runs."""
    _verify_token(x_api_key)

    payload = payload or {}
    agent_map = {
        "trend_hunter": ("agents.trend_hunter", "TrendHunterAgent"),
        "designer": ("agents.designer", "DesignerAgent"),
        "copywriter": ("agents.copywriter", "CopywriterAgent"),
        "publisher": ("agents.publisher", "PublisherAgent"),
        "client_manager": ("agents.client_manager", "ClientManagerAgent"),
        "sales_analyst": ("agents.sales_analyst", "SalesAnalystAgent"),
        "procurement": ("agents.procurement", "ProcurementAgent"),
        "accountant": ("agents.accountant", "AccountantAgent"),
    }

    if agent_name not in agent_map:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown agent. Valid: {list(agent_map.keys())}",
        )

    module_path, class_name = agent_map[agent_name]
    import importlib
    module = importlib.import_module(module_path)
    agent_class = getattr(module, class_name)
    agent = agent_class()

    # Run in background so HTTP response returns immediately
    asyncio.create_task(agent.run(payload))

    return {"status": "triggered", "agent": agent_name, "task": payload}


# ── Financial transaction endpoint ────────────────────────────

@app.post("/financials/record")
async def record_financial(
    transaction: dict,
    x_api_key: str = Header(None),
):
    """Record a financial transaction manually (sales, expenses, etc.)."""
    _verify_token(x_api_key)

    from agents.accountant import AccountantAgent
    agent = AccountantAgent()
    result = await agent.run({**transaction, "trigger": "record_transaction"})
    return result


# ── Avito webhook endpoint ─────────────────────────────────────

@app.post("/webhooks/avito")
async def avito_webhook(payload: dict):
    """
    Receive real-time events from Avito (new messages, order status changes).
    Register this URL in Avito Developer Console.
    """
    event_type = payload.get("event_type", "")
    log.info("webhook.avito", event_type=event_type)

    if event_type == "new_message":
        from agents.client_manager import ClientManagerAgent
        agent = ClientManagerAgent()
        asyncio.create_task(agent.run({"trigger": "message_poll"}))

    elif event_type == "order_status":
        order_id = payload.get("object_id")
        new_status = payload.get("new_status")
        log.info("webhook.order_status", order_id=order_id, status=new_status)

    return {"status": "received"}


# ── A/B tracking endpoint ──────────────────────────────────────

@app.post("/ab/record")
async def record_ab_event(
    data: dict,
    x_api_key: str = Header(None),
):
    """Record A/B test event: {listing_id, variant, event}"""
    _verify_token(x_api_key)
    from agents.copywriter import CopywriterAgent
    agent = CopywriterAgent()
    await agent.record_ab_result(
        data["listing_id"], data["variant"], data["event"]
    )
    return {"status": "ok"}


# ── Entry point ────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )


# ── Telegram client bot (runs alongside FastAPI) ──────────
import threading
import subprocess

def run_tg_bot():
    subprocess.run([sys.executable, "tg_agent.py"])

threading.Thread(target=run_tg_bot, daemon=True).start()

# Запускаем контент-агента
from content_agent import start_content_agent
start_content_agent()

# Запускаем print agent
def run_print_agent():
    subprocess.run([sys.executable, "print_agent.py"])

threading.Thread(target=run_print_agent, daemon=True).start()
