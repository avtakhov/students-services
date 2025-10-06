import datetime

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from db.event import EventDB


async def remove_events(
    event_ttl: datetime.timedelta,
    session: AsyncSession,
) -> int:
    cutoff = datetime.datetime.now(datetime.timezone.utc) - event_ttl
    result = await session.execute(delete(EventDB).where(EventDB.timestamp < cutoff))
    await session.commit()
    return result.rowcount or 0
