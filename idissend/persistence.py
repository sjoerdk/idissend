"""Classes and functions to be able to hold on to records beyond python executions"""
import sqlalchemy

from datetime import datetime
from idissend.exceptions import IDISSendException
from idissend.orm import Base, IDISRecord
from pathlib import Path
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine
from typing import List, Optional

Session = sessionmaker()


class IDISSendRecordsSession:
    """An open session to a records database

    Removes some of the clutter of sqlalchemy backend. Properly typed method
    signatures and handling of session close with context manager

    Examples
    --------
    with IDISSendRecordsSession(sqlalchemy_session) as session:
        session.do_things()

    Notes
    -----
    Session is closed automatically after 'with' context is left. Objects returned
    from IDISSendRecordsSession methods can still be used after session close,
    but need to be added again to new session to persist any changes

    See Also
    --------
    You can use IDISSendRecords to generate IDISSendRecordsSession objects

    """

    def __init__(self, session: sqlalchemy.orm.session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def close(self):
        """Close tries to keep as much information alive as possible in the objects
        returned by IDISSendRecordsSession methods.

        By default, sqlalchemy will invalidate any object fields after close.
        This can be stopped by expunge, but this only saves fields that have been
        accessed before. This is quite annoying here because sessions are quite
        short lived and often not all fields are accessed. Instead of hacking
        something to access all fields, the following amazing commands will do:
        """
        # flush all changes, obtain pk's etc, but do not commit
        self.session.flush()
        # detach all objects from session. Persists all object fields after close
        self.session.expunge_all()
        # Write to db. Without expunge all fields would now have become invalid
        self.session.commit()
        # be done with it
        self.session.close()

    def get_all(self) -> List[IDISRecord]:
        return self.session.query(IDISRecord).all()

    def get_for_study_folder(self, study_folder: Path) -> Optional[IDISRecord]:
        """Get record for the given study folder. Returns None if not found"""
        raise NotImplementedError("Use get_for_study_id!")

    def get_for_study_id(self, study_id: str) -> Optional[IDISRecord]:
        """Get first record for the given study id. Returns None if not found"""
        return (
            self.session.query(IDISRecord)
            .filter(IDISRecord.study_id == study_id)
            .first()
        )

    def get_for_job_id(self, job_id: int) -> Optional[IDISRecord]:
        """Get record for the given job_id. Returns None if not found"""
        return (
            self.session.query(IDISRecord).filter(IDISRecord.job_id == job_id).first()
        )

    def add(
        self,
        study_id: str,
        job_id: int,
        server_name: str,
        last_status: Optional[str] = None,
        last_check: Optional[datetime] = None,
    ) -> IDISRecord:
        """Create a records with the given parameters.

        Made this instead of just using IDISRecord to be explicit about
        argument types and default arguments
        """

        record = IDISRecord(
            study_id=study_id,
            job_id=job_id,
            server_name=server_name,
            last_status=last_status,
            last_check=last_check,
        )
        self.add_record(record)
        return record

    def add_record(self, record: IDISRecord):
        self.session.add(record)

    def delete(self, record: IDISRecord):
        self.session.delete(record)


def get_db_sessionmaker(db_url) -> sqlalchemy.orm.session.sessionmaker:
    """Returns a session on a anonqa sqlite database in the given file.
    Creates db if it does not exist

    Parameters
    ----------
    db_url: String
        Sqlalchemy database url.
        See https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls


    Returns
    -------
    sqlalchemy.orm.session.Session
        A session on the database in db_filename
    """
    engine = create_engine(db_url, echo=False)
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

    def get_session(self) -> IDISSendRecordsSession:
        return IDISSendRecordsSession(session=self.session_maker())


class DBLoadException(IDISSendException):
    pass
