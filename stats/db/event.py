from sqlalchemy import (
    Column,
    String,
    DateTime,
    Index,
    BigInteger,
    ForeignKey,
    Integer,
)

from db.base import Base


class User(Base):
    __tablename__ = "users"

    vk_user_id = Column(BigInteger, nullable=False, primary_key=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)

    __table_args__ = (
        Index("idx_users_vk_user_id", "vk_user_id"),
        Index("idx_users_name", "first_name", "last_name"),
    )


class UserQr(Base):
    __tablename__ = "user_qr"

    vk_user_id = Column(ForeignKey("users.vk_user_id"), primary_key=True)
    qr_id = Column(String, primary_key=True)

    __table_args__ = (
        Index("idx_user_qr_vk_user_id", "vk_user_id"),
        Index("idx_user_qr_qr_id", "qr_id"),
    )


class EventDB(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)

    event_type = Column(String(), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    vk_user_id = Column(ForeignKey("users.vk_user_id"), nullable=False)
    qr_id = Column(ForeignKey("user_qr.qr_id"), nullable=True)

    __table_args__ = (
        Index("idx_events_vk_user_id", "vk_user_id"),
        Index("idx_events_qr_id", "qr_id"),
        Index("idx_events_event_type", "event_type"),
        Index("idx_events_timestamp", "timestamp"),
        Index("idx_events_event_type_timestamp", "event_type", "timestamp"),
        Index("idx_events_type_ts_id", "event_type", "timestamp", "id"),
    )
