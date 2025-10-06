from fastapi import APIRouter

from routes import events
from routes import events_stats

router = APIRouter()
router.include_router(events.router, tags=["events"])
router.include_router(events_stats.router, tags=["events"])
