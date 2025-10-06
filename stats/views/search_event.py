from __future__ import annotations

import base64
import datetime as dt

import pydantic
from sqlalchemy import and_, or_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.event import EventDB, User
from models.event import Event, VkUserInfo, Payload


def _encode_cursor(ts: dt.datetime, id_: int) -> str:
    raw = f"{ts.isoformat()}|{id_}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("utf-8")


def _decode_cursor(cursor: str) -> tuple[dt.datetime, int]:
    raw = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
    ts_str, id_str = raw.split("|", 1)
    return dt.datetime.fromisoformat(ts_str), int(id_str)


class SearchEventResult(pydantic.BaseModel):
    events: list[Event]
    next_cursor: str | None
    has_more: bool


async def search_event(
    qr_id: str | None,
    from_: dt.datetime,
    to_: dt.datetime,
    session: AsyncSession,
    limit: int,
    cursor: str | None,
) -> SearchEventResult:
    conds = [
        EventDB.timestamp >= from_,
        EventDB.timestamp <= to_,
    ]

    # Добавляем условие qr_id только если оно указано
    if qr_id is not None:
        conds.append(EventDB.qr_id == qr_id)

    if cursor:
        cur_ts, cur_id = _decode_cursor(cursor)
        conds.append(
            or_(
                EventDB.timestamp < cur_ts,
                and_(EventDB.timestamp == cur_ts, EventDB.id < cur_id),
            )
        )

    stmt = (
        select(EventDB, User)
        .join(User, EventDB.vk_user_id == User.vk_user_id)
        .where(and_(*conds))
        .order_by(desc(EventDB.timestamp), desc(EventDB.id))
        .limit(max(1, int(limit)) + 1)
    )

    rows = (await session.execute(stmt)).all()
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    events: list[Event] = [
        Event(
            event_type=e.event_type,
            timestamp=e.timestamp,
            vk_user_info=VkUserInfo(
                id=u.vk_user_id,
                first_name=u.first_name,
                last_name=u.last_name,
            ),
            payload=Payload(qr_id=e.qr_id),
        )
        for (e, u) in rows
    ]

    next_cursor: str | None = None
    if has_more and rows:
        last_event, _ = rows[-1]
        next_cursor = _encode_cursor(last_event.timestamp, last_event.id)

    return SearchEventResult(
        events=events,
        next_cursor=next_cursor,
        has_more=has_more,
    )