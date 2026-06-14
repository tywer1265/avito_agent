# core/database.py
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional
import structlog
from sqlalchemy import (
    BigInteger, Column, DateTime, Float, Index, Integer,
    Numeric, String, Text, text,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from core.config import settings

log = structlog.get_logger(__name__)


# ── ORM Base ──────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Table models ──────────────────────────────────────────────

class AgentLog(Base):
    __tablename__ = "agents_log"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), default=_now, nullable=False)
    agent_name = Column(String(64), nullable=False)
    action = Column(String(128), nullable=False)
    input = Column(Text)
    output = Column(Text)
    cost = Column(Numeric(10, 6), default=0)
    confidence = Column(Float, default=1.0)
    status = Column(String(32), default="ok")
    __table_args__ = (
        Index("ix_agents_log_agent_ts", "agent_name", "timestamp"),
    )


class Trend(Base):
    __tablename__ = "trends"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(256), nullable=False)
    category = Column(String(64))
    score = Column(Integer, default=0)
    sources = Column(Text)          # JSON list of source names
    recommendation = Column(Text)
    created_at = Column(DateTime(timezone=True), default=_now)
    status = Column(String(32), default="new")  # new | approved | rejected | ordered


class Product(Base):
    __tablename__ = "products"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trend_id = Column(BigInteger)
    name = Column(String(256), nullable=False)
    category = Column(String(64))
    color = Column(String(64))
    price_rub = Column(Numeric(12, 2))
    cost_rub = Column(Numeric(12, 2))
    margin = Column(Float)
    status = Column(String(32), default="draft")  # draft | active | discontinued
    created_at = Column(DateTime(timezone=True), default=_now)


class Listing(Base):
    __tablename__ = "listings"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_id = Column(BigInteger)
    avito_id = Column(String(128))
    title = Column(String(128))
    description = Column(Text)
    views = Column(Integer, default=0)
    contacts = Column(Integer, default=0)
    status = Column(String(32), default="draft")  # draft | active | paused | sold
    published_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), default=_now)


class Order(Base):
    __tablename__ = "orders"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    listing_id = Column(BigInteger)
    buyer_name = Column(String(256))
    price = Column(Numeric(12, 2))
    status = Column(String(32), default="new")  # new | confirmed | shipped | done | cancelled
    created_at = Column(DateTime(timezone=True), default=_now)


class Inventory(Base):
    __tablename__ = "inventory"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_id = Column(BigInteger, unique=True)
    quantity = Column(Integer, default=0)
    reorder_threshold = Column(Integer, default=5)
    supplier = Column(String(256))
    updated_at = Column(DateTime(timezone=True), default=_now)


class Financial(Base):
    __tablename__ = "financials"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date = Column(DateTime(timezone=True), default=_now)
    type = Column(String(32))       # income | expense | commission | tax
    amount = Column(Numeric(12, 2))
    category = Column(String(64))
    description = Column(Text)
    agent_source = Column(String(64))


class Message(Base):
    __tablename__ = "messages"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    listing_id = Column(BigInteger)
    buyer_contact = Column(String(256))
    content = Column(Text)
    direction = Column(String(8))   # in | out
    responded_at = Column(DateTime(timezone=True))
    status = Column(String(32), default="new")  # new | replied | escalated | closed


# ── Engine & session factory ───────────────────────────────────

_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            echo=not settings.is_production,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(), expire_on_commit=False, class_=AsyncSession
        )
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ── Schema initialisation ─────────────────────────────────────

async def init_db() -> None:
    """Create all tables if they don't exist. Safe to call on every startup."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("database.init_db", status="ok", tables=list(Base.metadata.tables.keys()))


async def close_db() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
    log.info("database.close_db", status="ok")
