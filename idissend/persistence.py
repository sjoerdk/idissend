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


def get_db_sessionmaker(db_filename) -> sqlalchemy.orm.session.sessionmaker:
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
    return Session


def get_memory_only_sessionmaker() -> sqlalchemy.orm.session.sessionmaker:
    """Session on db that exists only in memory. Will stop existing when closed"""
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
    Session.configure(bind=engine)
    return Session


class IDISSendRecords:
    """A thing that holds persistent records for idissend

    This is the object that gets passed around in idisssend. When actual db
    transactions are needed, use get_session and the 'with' statement:

    records = IDISSendRecords(session_maker)

    with records.get_session() as session:
        session.do_things()

    """

    def __init__(self, session_maker: sqlalchemy.orm.session.sessionmaker):
        self.session_maker = session_maker

    def get_session(self):
        return IDISSendRecordsSession(session=self.session_maker())


class IDISSendRecordsSession:
    """An open session to a records database

    Removes some of the clutter of sqlalchemy backend. Properly typed method
    signatures and handling of session close with context manager"""

    def __init__(self, session: sqlalchemy.orm.session):
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
        """Get record for the given study folder. Returns None if not found"""
        return self.session.query(PendingAnonRecord).filter(
            PendingAnonRecord.study_folder == study_folder).first()

    def get_for_job_id(self, job_id: int) -> Optional[PendingAnonRecord]:
        """Get record for the given job_id. Returns None if not found"""
        return self.session.query(PendingAnonRecord).filter(
            PendingAnonRecord.job_id == job_id).first()

    def add(
            self,
            study_folder: Path,
            job_id: int,
            server_name: str,
            last_status: Optional[str] = None,
            last_check: Optional[datetime] = None,
    ) -> PendingAnonRecord:

        record = PendingAnonRecord(study_folder=study_folder,
                                   job_id=job_id,
                                   server_name=server_name,
                                   last_status=last_status,
                                   last_check=last_check)
        self.session.add(record)
        return record

    def delete(self, record: PendingAnonRecord):
        self.session.delete(record)




class DBLoadException(IDISSendException):
    pass
