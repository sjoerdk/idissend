"""Object Relational Map for mapping idissend objects to database tables

"""
from pathlib import Path
from sqlalchemy import types
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime
from sqlalchemy.sql.type_api import TypeDecorator

Base = declarative_base()


class PathType(TypeDecorator):
    """A sqlalchemy type for pathlib.Path instances"""

    impl = types.String

    def process_bind_param(self, value, dialect):
        return str(value)

    def process_literal_param(self, value, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        return Path(value)


class PendingAnonRecord(Base):
    """Records for studies that er pending anonymization"""

    __tablename__ = "pending_anon"

    id = Column(Integer, primary_key=True)
    study_folder = Column(PathType)
    job_id = Column(Integer, unique=True)
    server_name = Column(String(length=256))
    last_status = Column(String(length=128))
    last_check = Column(DateTime)
