from datetime import datetime
from pathlib import Path

from idissend.orm import PendingAnonRecord
from idissend.persistence import get_db_sessionmaker, IDISSendRecords


def test_db(tmpdir):
    """Just very basic object creation in db. Check that it works as expected"""

    db_file = Path(tmpdir) / "test_db.sqlite"
    session = get_db_sessionmaker(db_file)()

    study_folder = Path("/tmp/a_path")
    job_id = 123
    server_name = "p01"
    last_check = datetime.now()
    last_status = 4

    assert not session.query(PendingAnonRecord).all()
    a_record = PendingAnonRecord(
        study_folder=study_folder, job_id=job_id, server_name=server_name
    )
    session.add(a_record)
    session.commit()
    session.close()

    session = get_db_sessionmaker(db_file)()
    test = session.query(PendingAnonRecord).first()
    assert not test.last_check
    assert not test.last_status

    test.last_check = last_check
    test.last_status = last_status

    session.add(test)
    session.commit()
    session.close()

    session = get_db_sessionmaker(db_file)()
    test = session.query(PendingAnonRecord).first()
    assert test.study_folder == study_folder
    assert test.last_check == last_check


def test_idis_send_records(tmpdir):
    """the records object makes interacting with db slightly cleaner """

    db_file = Path(tmpdir) / "test_db.sqlite"
    records = IDISSendRecords(session_maker=get_db_sessionmaker(db_file))

    with records.get_session() as session:
        assert not session.get_all()
        session.add(study_folder=Path("test/something"), job_id=99, server_name="p03")

        session.add(study_folder=Path("test/something2"), job_id=100, server_name="p03")

    with records.get_session() as session:
        assert len(session.get_all()) == 2
        assert (
            session.get_for_study_folder(study_folder=Path("test/something2")).job_id
            == 100
        )
        assert not session.get_for_study_folder(study_folder=Path("test/something5"))

        session.delete(
            session.get_for_study_folder(study_folder=Path("test/something2"))
        )

    with records.get_session() as session:
        assert len(session.get_all()) == 1
        assert session.get_for_job_id(99)
        assert not session.get_for_job_id(100)


def test_object_field_persistence(tmpdir):
    """Got into sqlalchemy object state trouble again.
    So when you create an ORM object, add it to as session, Expunge the object,
     close the session, why are all the object's fields inaccessible?

    """
    db_file = Path(tmpdir) / "test_db.sqlite"
    records = IDISSendRecords(session_maker=get_db_sessionmaker(db_file))

    with records.get_session() as session:
        record = session.add(
            study_folder=Path("test"), job_id=1, server_name="test_server"
        )
    _ = record.server_name  # This should not raise an unbound exception

    # now can be change this unbound object and then commit the results?
    record.server_name = "changed"
    with records.get_session() as session:
        session.add_record(record)
    # yes
