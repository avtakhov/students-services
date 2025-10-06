import datetime

from db.base import get_sqlite_session
from views.remove_events import remove_events


async def cleanup_events_worker():
    async with get_sqlite_session() as session:
        await remove_events(datetime.timedelta(days=30), session)
