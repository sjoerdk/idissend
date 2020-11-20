"""Object Relational Map for mapping idissend objects to database tables"""
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime


Base = declarative_base()


class IDISRecord(Base):
    """Records for studies that are connected to an IDIS anonymization job"""

    __tablename__ = "idis_record"

    id = Column(Integer, primary_key=True)
    study_id = Column(String(length=256))  # unique identifier within idissend
    job_id = Column(Integer, unique=True)
    server_name = Column(String(length=256))
    last_status = Column(String(length=128))
    last_error_message = Column(String(length=1024))
    last_check = Column(DateTime)
