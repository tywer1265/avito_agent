# core/scheduler.py
"""
APScheduler setup. All agent schedules are registered here.
Timezone: Europe/Moscow (MSK = UTC+3).
"""
from __future__ import annotations

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone as tz

from core.config import settings

log = structlog.get_logger(__name__)

MSK = tz("Europe/Moscow")

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone=MSK)
    return _scheduler


def setup_schedules(scheduler: AsyncIOScheduler) -> None:
    """Register all agent jobs. Called once at startup from main.py."""
    from agents.trend_hunter import TrendHunterAgent
    from agents.designer import DesignerAgent
    from agents.copywriter import CopywriterAgent
    from agents.publisher import PublisherAgent
    from agents.client_manager import ClientManagerAgent
    from agents.sales_analyst import SalesAnalystAgent
    from agents.procurement import ProcurementAgent
    from agents.accountant import AccountantAgent

    trend_hunter = TrendHunterAgent()
    designer = DesignerAgent()
    copywriter = CopywriterAgent()
    publisher = PublisherAgent()
    client_manager = ClientManagerAgent()
    analyst = SalesAnalystAgent()
    procurement = ProcurementAgent()
    accountant = AccountantAgent()

    # ── TREND HUNTER — every Monday 09:00 MSK ─────────────────
    scheduler.add_job(
        lambda: _run(trend_hunter, {"trigger": "weekly_schedule"}),
        CronTrigger(day_of_week="mon", hour=9, minute=0, timezone=MSK),
        id="trend_hunter_weekly",
        name="Trend Hunter — Weekly Scan",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # ── DESIGNER — triggered by trend_hunter via DB flag; also sweep hourly ─
    scheduler.add_job(
        lambda: _run(designer, {"trigger": "pending_products_sweep"}),
        CronTrigger(minute=30, timezone=MSK),   # every hour at :30
        id="designer_hourly",
        name="Designer — Pending Products Sweep",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── COPYWRITER — sweep for products needing listings ──────
    scheduler.add_job(
        lambda: _run(copywriter, {"trigger": "pending_listings_sweep"}),
        CronTrigger(minute=45, timezone=MSK),
        id="copywriter_hourly",
        name="Copywriter — Pending Listings Sweep",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── PUBLISHER — post at peak hours ────────────────────────
    for hour in settings.peak_hours_list:
        scheduler.add_job(
            lambda h=hour: _run(publisher, {"trigger": "peak_hour_post", "hour": h}),
            CronTrigger(hour=hour, minute=5, timezone=MSK),
            id=f"publisher_peak_{hour}",
            name=f"Publisher — Peak Post {hour:02d}:05",
            replace_existing=True,
            misfire_grace_time=300,
        )

    # Publisher also refreshes listings mid-day
    scheduler.add_job(
        lambda: _run(publisher, {"trigger": "listing_refresh"}),
        CronTrigger(hour=14, minute=0, timezone=MSK),
        id="publisher_refresh",
        name="Publisher — Listing Refresh",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── CLIENT MANAGER — poll for new messages every 3 minutes ─
    scheduler.add_job(
        lambda: _run(client_manager, {"trigger": "message_poll"}),
        CronTrigger(minute="*/3", timezone=MSK),
        id="client_manager_poll",
        name="Client Manager — Message Poll",
        replace_existing=True,
        misfire_grace_time=60,
    )

    # Follow-up sweep: once daily at 11:00 MSK
    scheduler.add_job(
        lambda: _run(client_manager, {"trigger": "followup_sweep"}),
        CronTrigger(hour=11, minute=0, timezone=MSK),
        id="client_manager_followup",
        name="Client Manager — Follow-up Sweep",
        replace_existing=True,
        misfire_grace_time=1800,
    )

    # ── SALES ANALYST — daily report 20:00 MSK ───────────────
    scheduler.add_job(
        lambda: _run(analyst, {"trigger": "daily_report"}),
        CronTrigger(hour=20, minute=0, timezone=MSK),
        id="analyst_daily_report",
        name="Sales Analyst — Daily Report",
        replace_existing=True,
        misfire_grace_time=1800,
    )

    # Competitor monitoring: daily at 10:00 MSK
    scheduler.add_job(
        lambda: _run(analyst, {"trigger": "competitor_scan"}),
        CronTrigger(hour=10, minute=0, timezone=MSK),
        id="analyst_competitor_scan",
        name="Sales Analyst — Competitor Scan",
        replace_existing=True,
        misfire_grace_time=1800,
    )

    # Conversion drop check: every 2 hours
    scheduler.add_job(
        lambda: _run(analyst, {"trigger": "conversion_check"}),
        CronTrigger(minute=0, hour="*/2", timezone=MSK),
        id="analyst_conversion_check",
        name="Sales Analyst — Conversion Check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # ── PROCUREMENT — weekly review Tuesday 10:00 MSK ─────────
    scheduler.add_job(
        lambda: _run(procurement, {"trigger": "weekly_review"}),
        CronTrigger(day_of_week="tue", hour=10, minute=0, timezone=MSK),
        id="procurement_weekly",
        name="Procurement — Weekly Review",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Inventory check: daily at 08:00 MSK
    scheduler.add_job(
        lambda: _run(procurement, {"trigger": "inventory_check"}),
        CronTrigger(hour=8, minute=0, timezone=MSK),
        id="procurement_inventory",
        name="Procurement — Inventory Check",
        replace_existing=True,
        misfire_grace_time=1800,
    )

    # ── ACCOUNTANT — daily P&L 21:00 MSK ─────────────────────
    scheduler.add_job(
        lambda: _run(accountant, {"trigger": "daily_pnl"}),
        CronTrigger(hour=21, minute=0, timezone=MSK),
        id="accountant_daily",
        name="Accountant — Daily P&L",
        replace_existing=True,
        misfire_grace_time=1800,
    )

    # Weekly summary: Monday 08:00 MSK (before trend hunter fires)
    scheduler.add_job(
        lambda: _run(accountant, {"trigger": "weekly_summary"}),
        CronTrigger(day_of_week="mon", hour=8, minute=0, timezone=MSK),
        id="accountant_weekly",
        name="Accountant — Weekly Summary",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Monthly report: 1st of month 07:00 MSK
    scheduler.add_job(
        lambda: _run(accountant, {"trigger": "monthly_report"}),
        CronTrigger(day=1, hour=7, minute=0, timezone=MSK),
        id="accountant_monthly",
        name="Accountant — Monthly Report",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    log.info("scheduler.setup_schedules", jobs=len(scheduler.get_jobs()))


async def _run(agent, task: dict) -> None:
    """Fire-and-forget wrapper — scheduler callbacks must be sync lambdas."""
    import asyncio
    try:
        await agent.run(task)
    except Exception as exc:
        log.error("scheduler.job_error", agent=agent.name, error=str(exc))
