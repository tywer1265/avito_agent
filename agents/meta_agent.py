"""
meta_agent.py — Meta Agent (оркестратор цепочек агентов)

Координирует pipeline'ы вида:
    TrendHunter -> Designer -> Copywriter -> Publisher

Философия (по agent-thinking):
  - Структурированные выходы между шагами (dict-контекст, не free-form текст)
  - Валидация на каждом шаге (роль "критика"): мусор не едет дальше
  - Ретраи с экспоненциальным backoff на transient-сбои
  - Стоп-цепочка при фатальной ошибке + алерт владельцу
  - Полная наблюдаемость: каждый шаг логируется
  - Килсвитч: можно остановить публикацию одной переменной окружения

====================================================================
ДОПУЩЕНИЯ (проверь и поправь под свой код — отмечено # ASSUMPTION)
====================================================================
1. MetaAgent наследует BaseAgent из core.base_agent.
2. У BaseAgent есть async/sync методы:
       self.log(level: str, message: str, **extra)        -> запись в PostgreSQL
       self.report_to_telegram(text: str)                 -> алерт владельцу
   Если они синхронные — убери await перед ними (см. _alog / _areport).
3. Шаги pipeline — это async-функции вида:
       async def step(ctx: dict) -> dict
   которые принимают накопленный контекст и возвращают свой результат (dict).
   Свои агенты ты оборачиваешь в такие функции (примеры внизу файла).
4. Килсвитч: переменная окружения META_AGENT_PUBLISH_ENABLED.
   Если != "1" — шаг с флагом is_publish=True не выполнится (цепочка
   дойдёт до него, подготовит всё, но НЕ выложит и предупредит тебя).
====================================================================
"""

from __future__ import annotations

import os
import asyncio
import inspect
import time
import traceback
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from core.base_agent import BaseAgent  # ASSUMPTION: путь к BaseAgent


# --- типы ----------------------------------------------------------

# Шаг pipeline: принимает контекст, возвращает свой кусок результата.
StepFn = Callable[[dict], Awaitable[dict]]

# Валидатор: смотрит на результат шага, возвращает (ok, причина).
# Это роль "критика" из agent-thinking — мусор не должен ехать дальше.
ValidatorFn = Callable[[dict], "tuple[bool, str]"]


@dataclass
class Step:
    """Один шаг цепочки."""
    name: str
    fn: StepFn
    validator: Optional[ValidatorFn] = None
    is_publish: bool = False          # шаг затрагивает реальную витрину Авито
    max_retries: int = 2              # сколько раз ретраить transient-сбой
    timeout_sec: float = 90.0         # таймаут на шаг


@dataclass
class Pipeline:
    """Именованная цепочка шагов."""
    name: str
    steps: list[Step] = field(default_factory=list)


@dataclass
class StepOutcome:
    """Результат выполнения одного шага — для лога и отчёта."""
    name: str
    ok: bool
    attempts: int
    duration_sec: float
    error: Optional[str] = None
    output_keys: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Итог всей цепочки."""
    pipeline: str
    ok: bool
    context: dict
    outcomes: list[StepOutcome] = field(default_factory=list)
    failed_step: Optional[str] = None

    def summary(self) -> str:
        lines = [f"Pipeline «{self.pipeline}»: {'✅ OK' if self.ok else '❌ FAILED'}"]
        for o in self.outcomes:
            mark = "✅" if o.ok else "❌"
            extra = f" ({o.attempts} попыт., {o.duration_sec:.1f}с)"
            line = f"  {mark} {o.name}{extra}"
            if o.error:
                line += f" — {o.error}"
            lines.append(line)
        if self.failed_step:
            lines.append(f"  Остановлено на: {self.failed_step}")
        return "\n".join(lines)


# --- Meta Agent ----------------------------------------------------

class MetaAgent(BaseAgent):
    """
    Оркестратор. Регистрируешь pipeline'ы через register_pipeline(),
    запускаешь через run_pipeline(name, initial_context).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pipelines: dict[str, Pipeline] = {}

    # -- регистрация --------------------------------------------

    def register_pipeline(self, pipeline: Pipeline) -> None:
        self._pipelines[pipeline.name] = pipeline

    # -- запуск -------------------------------------------------

    async def run_pipeline(
        self,
        name: str,
        initial_context: Optional[dict] = None,
    ) -> PipelineResult:
        pipeline = self._pipelines.get(name)
        if pipeline is None:
            raise KeyError(f"Pipeline «{name}» не зарегистрирован")

        context: dict = dict(initial_context or {})
        result = PipelineResult(pipeline=name, ok=True, context=context)

        await self._alog("info", f"▶️ Старт pipeline «{name}»")

        for step in pipeline.steps:
            # Килсвитч для публикующих шагов.
            if step.is_publish and not _publish_enabled():
                msg = (
                    f"⏸️ Шаг «{step.name}» пропущен: килсвитч "
                    f"META_AGENT_PUBLISH_ENABLED != 1. Карточка подготовлена, "
                    f"но НЕ опубликована."
                )
                await self._alog("warning", msg)
                await self._areport(f"⚠️ {name}: {msg}")
                result.outcomes.append(
                    StepOutcome(step.name, ok=False, attempts=0,
                                duration_sec=0.0, error="publish kill-switch")
                )
                result.ok = False
                result.failed_step = step.name
                return result

            outcome = await self._run_step(step, context)
            result.outcomes.append(outcome)

            if not outcome.ok:
                # Стоп-цепочка: дальше не едем, чтобы не выложить брак.
                result.ok = False
                result.failed_step = step.name
                alert = (
                    f"🛑 Pipeline «{name}» остановлен на шаге «{step.name}».\n"
                    f"Причина: {outcome.error}\n\n{result.summary()}"
                )
                await self._alog("error", alert)
                await self._areport(alert)
                return result

        await self._alog("info", f"✅ Pipeline «{name}» завершён успешно")
        await self._areport(f"✅ {name} — готово.\n\n{result.summary()}")
        return result

    # -- выполнение одного шага с ретраями ----------------------

    async def _run_step(self, step: Step, context: dict) -> StepOutcome:
        start = time.monotonic()
        last_error = ""

        for attempt in range(1, step.max_retries + 2):  # 1 + retries
            try:
                await self._alog(
                    "info",
                    f"  → шаг «{step.name}» попытка {attempt}",
                )
                # Таймаут на шаг — защита от зависших вызовов модели/API.
                output = await asyncio.wait_for(
                    step.fn(context), timeout=step.timeout_sec
                )

                if not isinstance(output, dict):
                    raise TypeError(
                        f"шаг вернул {type(output).__name__}, ожидался dict"
                    )

                # Роль критика: валидируем результат до того, как пустить дальше.
                if step.validator is not None:
                    ok, reason = step.validator(output)
                    if not ok:
                        raise ValueError(f"валидация не пройдена: {reason}")

                # Сливаем результат шага в общий контекст.
                context.update(output)

                duration = time.monotonic() - start
                return StepOutcome(
                    name=step.name, ok=True, attempts=attempt,
                    duration_sec=duration, output_keys=list(output.keys()),
                )

            except asyncio.TimeoutError:
                last_error = f"таймаут {step.timeout_sec}с"
            except (ValueError, TypeError) as e:
                # Логические ошибки/валидация — обычно ретрай не спасёт,
                # но один повтор дадим (модель могла дать кривой ответ).
                last_error = str(e)
            except Exception as e:  # transient: сеть, 5xx, rate limit
                last_error = f"{type(e).__name__}: {e}"
                await self._alog(
                    "warning",
                    f"  шаг «{step.name}» упал: {last_error}\n"
                    f"{traceback.format_exc(limit=3)}",
                )

            # Экспоненциальный backoff перед следующей попыткой.
            if attempt <= step.max_retries:
                backoff = min(2 ** (attempt - 1), 8)
                await asyncio.sleep(backoff)

        duration = time.monotonic() - start
        return StepOutcome(
            name=step.name, ok=False, attempts=step.max_retries + 1,
            duration_sec=duration, error=last_error,
        )

    # -- адаптеры под sync/async BaseAgent ----------------------
    # Если твои log/report_to_telegram синхронные — эти обёртки всё равно
    # сработают. Если async — тоже. Менять ничего не нужно.

    async def _alog(self, level: str, message: str) -> None:
        await _maybe_await(self.log, level, message)

    async def _areport(self, text: str) -> None:
        await _maybe_await(self.report_to_telegram, text)


# --- утилиты -------------------------------------------------------

def _publish_enabled() -> bool:
    return os.getenv("META_AGENT_PUBLISH_ENABLED", "0") == "1"


async def _maybe_await(fn: Callable, *args, **kwargs):
    """Вызывает fn; если результат awaitable — ждёт его. Sync/async агностик."""
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# ===================================================================
# ПРИМЕР СБОРКИ ЦЕПОЧКИ ВЫКЛАДКИ
# Это шаблон. Подставь вызовы своих реальных агентов внутрь функций-шагов.
# ===================================================================
#
# from agents.trend_hunter import TrendHunter
# from agents.designer import Designer
# from agents.copywriter import Copywriter
# from agents.publisher import Publisher
#
# trend, designer, copy, publisher = TrendHunter(), Designer(), Copywriter(), Publisher()
#
# async def step_trend(ctx: dict) -> dict:
#     # ASSUMPTION: у агента есть метод, возвращающий выбранную позицию.
#     item = await trend.pick_item_from_stock()   # ← твой реальный метод
#     return {"item": item}
#
# async def step_design(ctx: dict) -> dict:
#     image_url = await designer.make_card(ctx["item"])
#     return {"image_url": image_url}
#
# async def step_copy(ctx: dict) -> dict:
#     listing = await copy.write_listing(ctx["item"])  # title/description/price
#     return {"listing": listing}
#
# async def step_publish(ctx: dict) -> dict:
#     avito_id = await publisher.publish(ctx["item"], ctx["image_url"], ctx["listing"])
#     return {"avito_listing_id": avito_id}
#
# # Валидаторы (роль критика) — отсекают мусор до публикации:
# def v_item(o):    return (bool(o.get("item")), "пустая позиция")
# def v_image(o):   return (str(o.get("image_url","")).startswith("http"), "нет URL картинки")
# def v_listing(o):
#     l = o.get("listing") or {}
#     if not l.get("title"):       return (False, "пустой заголовок")
#     if not l.get("description"): return (False, "пустое описание")
#     price = l.get("price", 0)
#     if not (100 <= price <= 100000): return (False, f"подозрительная цена {price}")
#     return (True, "")
#
# listing_pipeline = Pipeline(
#     name="avito_listing",
#     steps=[
#         Step("trend_hunter", step_trend,  validator=v_item),
#         Step("designer",     step_design, validator=v_image),
#         Step("copywriter",   step_copy,   validator=v_listing),
#         Step("publisher",    step_publish, is_publish=True),  # под килсвитчем
#     ],
# )
#
# # В scheduler.py:
# # meta = MetaAgent(...)              # как ты инициализируешь остальные агенты
# # meta.register_pipeline(listing_pipeline)
# # await meta.run_pipeline("avito_listing")
