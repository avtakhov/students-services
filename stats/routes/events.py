from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.base import get_sqlite_session
from models.event import Event
from views.add_event import add_event

router = APIRouter()


@router.post("/events")
async def post_events(
    event: Event,
    session: AsyncSession = Depends(get_sqlite_session),
):
    await add_event(event, session)

    return {}
