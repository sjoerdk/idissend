"""Object Relational Map for mapping idissend objects to database tables"""
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime


Base = declarative_base()


class PendingAnonRecord(Base):
    """Records for studies that are pending anonymization"""

    __tablename__ = "pending_anon"

    id = Column(Integer, primary_key=True)
    study_id = Column(String(length=256))  # unique identifier within idissend
    job_id = Column(Integer, unique=True)
    server_name = Column(String(length=256))
    last_status = Column(String(length=128))
    last_check = Column(DateTime)
