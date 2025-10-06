import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_async_engine(os.getenv("DATABASE_URL", "sqlite+aiosqlite:///events.db"), echo=False)
SQLiteAsyncSession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

Base = declarative_base()


async def get_sqlite_session() -> AsyncGenerator[AsyncSession, None]:
    async with SQLiteAsyncSession() as session:
        yield session
