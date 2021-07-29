from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

maker = sessionmaker(autoflush=True, autocommit=False)
DBSession = scoped_session(maker)

DeclarativeBase: Any = declarative_base()
# for another schema
# declarative_base(metadata=MetaData(schema='public'))
metadata = DeclarativeBase.metadata


def init_model(engine):  # pragma: nocover
    """Call me before using any of the tables or classes in the model."""
    DBSession.configure(bind=engine)


def init_database(url, echo=False):  # pragma: nocover
    engine = create_engine(url, echo=echo)
    init_model(engine)
    metadata.create_all(engine)


# Import your model modules here.
from owl_dev.database.model import Info  # noqa  # isort:skip
