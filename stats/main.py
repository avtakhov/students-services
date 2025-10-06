from contextlib import asynccontextmanager
from typing import AsyncIterator

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

import routes
from db.base import Base, engine
from views.workers import setup_scheduler

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    setup_scheduler(scheduler)
    scheduler.start()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield
    finally:
        scheduler.shutdown(wait=True)
        await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(routes.router)
