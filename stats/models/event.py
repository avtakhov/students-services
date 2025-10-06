import datetime
from enum import StrEnum

import pydantic


class EventType(StrEnum):
    Link = "Link"
    Subscribe = "Subscribe"
    Unsubscribe = "Unsubscribe"


class VkUserInfo(pydantic.BaseModel):
    id: int
    first_name: str
    last_name: str


class Payload(pydantic.BaseModel):
    qr_id: str | None = None


class Event(pydantic.BaseModel):
    event_type: EventType
    timestamp: datetime.datetime
    vk_user_info: VkUserInfo
    payload: Payload
