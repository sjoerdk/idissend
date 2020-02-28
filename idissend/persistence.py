"""Classes and functions to be able to hold on to records beyond python executions
"""
import pickle
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import sqlalchemy

from idissend.exceptions import IDISSendException
from idissend.orm import Base, PendingAnonRecord
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine

Session = sessionmaker()


def get_session(db_filename) -> sqlalchemy.orm.session:
    """Returns a session on a anonqa sqlite database in the given file.
    Creates db if it does not exist

    Returns
    -------
    sqlalchemy.orm.session.Session
        A session on the database in db_filename
    """
    engine = create_engine(f"sqlite:///{db_filename}", echo=False)
    Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
    Session.configure(bind=engine)
    return Session()


def get_memory_only_session() -> sqlalchemy.orm.session:
    """Session on db that exists only in memory. Will stop existing when closed"""
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
    Session.configure(bind=engine)
    return Session()


class IDISSendRecords:
    """A thing that holds persistent records for idissend

    Removes some of the clutter of sqlalchemy backend. Properly typed method
    signatures and handling of session with context manager
    """

    def __init__(self, session: sqlalchemy.orm.session.Session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def close(self):
        self.session.commit()
        self.session.close()

    def get_all(self) -> List[PendingAnonRecord]:
        return self.session.query(PendingAnonRecord).all()

    def get_for_study_folder(self, study_folder: Path) -> Optional[PendingAnonRecord]:
        return self.session.query(PendingAnonRecord).filter(
            PendingAnonRecord.study_folder == study_folder).first()

    def get_for_job_id(self, job_id: int) -> Optional[PendingAnonRecord]:
        return self.session.query(PendingAnonRecord).filter(
            PendingAnonRecord.job_id == job_id).first()

    def add(
        self,
        study_folder: Path,
        job_id: int,
        server_name: str,
        last_status: Optional[int] = None,
        last_check: Optional[datetime] = None,
    ):

        self.session.add(PendingAnonRecord(study_folder=study_folder,
                                           job_id=job_id,
                                           server_name=server_name,
                                           last_status=last_status,
                                           last_check=last_check))

    def delete(self, record: PendingAnonRecord):
        self.session.delete(record)


class DBLoadException(IDISSendException):
    pass
