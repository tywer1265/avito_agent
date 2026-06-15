"""
agents/listing_pipeline.py — сборка цепочки выкладки для Meta Agent.

Реальная архитектура: агенты координируются через БД триггерами-свипами,
а НЕ передачей данных через контекст.

    TrendHunter  trigger=(нет)               -> пишет таблицу Trend (сигнал)
    Designer     trigger=pending_products_sweep  -> draft Products -> картинки
    Copywriter   trigger=pending_listings_sweep  -> создаёт тексты/Listing
    Publisher    trigger=peak_hour_post          -> постит draft Listings

ВАЖНО: TrendHunter пишет в Trend, а Designer читает Product. Прямого звена
"тренд -> товар" нет (это Procurement/руками). Поэтому цепочка работает как
ежедневный свип того, что УЖЕ лежит в БД как draft.

Использование (scheduler.py):
    from agents.meta_agent import MetaAgent
    from agents.listing_pipeline import build_listing_pipeline
    meta = MetaAgent()
    meta.register_pipeline(build_listing_pipeline())
    await meta.run_pipeline("avito_listing")
"""
from __future__ import annotations

from agents.meta_agent import Pipeline, Step

from agents.trend_hunter import TrendHunterAgent
from agents.designer import DesignerAgent
from agents.copywriter import CopywriterAgent
from agents.publisher import PublisherAgent


def _unwrap(res: dict, agent: str) -> dict:
    """agent.run() не бросает исключений — конвертим status=error в исключение,
    чтобы Meta Agent отработал ретрай/стоп."""
    if not isinstance(res, dict):
        raise RuntimeError(f"{agent}: вернул не dict, а {type(res).__name__}")
    if res.get("status") == "error":
        raise RuntimeError(f"{agent}: {res.get('error', 'unknown error')}")
    return res


# --- инстансы агентов (один раз) -----------------------------------
_trend = TrendHunterAgent()
_designer = DesignerAgent()
_copywriter = CopywriterAgent()
_publisher = PublisherAgent()


# --- шаги ----------------------------------------------------------

async def step_trend(ctx: dict) -> dict:
    res = _unwrap(await _trend.run({}), "trend_hunter")
    return {"trends": res.get("trends", [])}


async def step_design(ctx: dict) -> dict:
    res = _unwrap(
        await _designer.run({"trigger": "pending_products_sweep"}),
        "designer",
    )
    return {"designed": res.get("processed", 0)}


async def step_copy(ctx: dict) -> dict:
    res = _unwrap(
        await _copywriter.run({"trigger": "pending_listings_sweep"}),
        "copywriter",
    )
    return {"copywritten": res.get("processed", 0)}


async def step_publish(ctx: dict) -> dict:
    res = _unwrap(
        await _publisher.run({"trigger": "peak_hour_post"}),
        "publisher",
    )
    return {"published": res.get("published", 0), "failed": res.get("failed", 0)}


# --- сборка --------------------------------------------------------
# Валидаторов по количеству НЕ ставим: "0 обработано" — это не ошибка,
# а просто пустой свип (нечего делать). Стоп-цепочку даёт только реальный
# сбой агента (status=error). Публикация под килсвитчем.

def build_listing_pipeline() -> Pipeline:
    return Pipeline(
        name="avito_listing",
        steps=[
            Step("trend_hunter", step_trend),
            Step("designer",     step_design),
            Step("copywriter",   step_copy),
            Step("publisher",    step_publish, is_publish=True),
        ],
    )
