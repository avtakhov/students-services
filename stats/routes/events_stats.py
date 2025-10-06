import datetime
import base64

import pydantic
from fastapi import APIRouter
from fastapi.params import Depends
from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession

from db.base import get_sqlite_session
from models.event import Event
from views.search_event import search_event

router = APIRouter()


class SearchRequest(pydantic.BaseModel):
    qr_id: str
    from_: datetime.datetime | None = Field(default=None, alias="from")
    to_: datetime.datetime | None = Field(default=None, alias="to")
    limit: int = 100
    cursor: str | None = None


class SearchResponse(pydantic.BaseModel):
    events: list[Event]
    has_more: bool = False
    next_cursor: str | None = None


@router.post("/events/search", response_model=SearchResponse)
async def post_events_stats(
    request: SearchRequest,
    session: AsyncSession = Depends(get_sqlite_session),
) -> SearchResponse:
    return SearchResponse.model_validate(
        obj=await search_event(
            request.qr_id,
            from_=request.from_,
            to_=request.to_,
            session=session,
            cursor=request.cursor,
            limit=request.limit,
        ),
        from_attributes=True,
    )
