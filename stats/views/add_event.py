from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.event import EventDB, User, UserQr
from models.event import Event, VkUserInfo

from sqlalchemy.dialects.sqlite import insert as sqlite_insert


async def _add_user(user: VkUserInfo, session: AsyncSession):
    await session.execute(
        sqlite_insert(User)
        .values(
            vk_user_id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
        )
        .on_conflict_do_nothing(index_elements=[User.vk_user_id])
    )


async def _add_user_qr(vk_user_id: int, qr_id: str, session: AsyncSession):
    await session.execute(
        sqlite_insert(UserQr)
        .values(
            vk_user_id=vk_user_id,
            qr_id=qr_id,
        )
        .on_conflict_do_update(
            index_elements=[UserQr.vk_user_id, UserQr.qr_id],
            set_={
                "qr_id": UserQr.qr_id,
            },
        )
    )


async def _get_user_qr(vk_user_id: int, session: AsyncSession) -> str | None:
    result = await session.execute(
        select(UserQr.qr_id).where(UserQr.vk_user_id == vk_user_id)
    )
    return result.scalar_one_or_none()


async def add_event(
    event: Event,
    session: AsyncSession,
):
    try:
        await _add_user(event.vk_user_info, session)
        if event.payload.qr_id is not None:
            await _add_user_qr(
                event.vk_user_info.id,
                event.payload.qr_id,
                session,
            )
        else:
            event.payload.qr_id = await _get_user_qr(
                event.vk_user_info.id,
                session,
            )

        session.add(
            EventDB(
                event_type=event.event_type,
                timestamp=event.timestamp,
                vk_user_id=event.vk_user_info.id,
                qr_id=event.payload.qr_id,
            )
        )

        await session.commit()
    except SQLAlchemyError:
        await session.rollback()
        raise
