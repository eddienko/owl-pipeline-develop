from owl_dev.database import DeclarativeBase
from sqlalchemy import Column, DateTime, Integer, Sequence, Unicode


class Info(DeclarativeBase):
    __tablename__ = "info"
    id = Column(Integer, Sequence("info_id_seq"), primary_key=True)
    config = Column(Unicode, nullable=False)
    env = Column(Unicode, nullable=False)
    python = Column(Unicode, nullable=False)
