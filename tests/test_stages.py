from pathlib import Path
from unittest.mock import Mock

import pytest
from anonapi.testresources import RemoteAnonServerFactory, JobInfoFactory, JobStatus, \
    MockAnonClientTool

from idissend.core import StudyPushException, PushStudyCallbackException
from idissend.persistence import IDISSendRecords, get_memory_only_sessionmaker
from idissend.stages import PendingAnon, IDISConnection, UnknownServerException


@pytest.fixture
def mock_anon_client_tool(monkeypatch):
    """An anonymization API client tool that does not hit the server but returns
    some example responses instead. Also records calls"""
    some_responses = [
        JobInfoFactory(status=JobStatus.DONE),
        JobInfoFactory(status=JobStatus.ERROR),
        JobInfoFactory(status=JobStatus.INACTIVE),
    ]
    # mock wrapper to be able to record responses
    return Mock(wraps=MockAnonClientTool(responses=some_responses))


@pytest.fixture
def an_idis_connection(mock_anon_client_tool):
    """An idis connection that mocks repsonses and does not hit any server"""
    return IDISConnection(
        client_tool=mock_anon_client_tool,
        servers=[RemoteAnonServerFactory(), RemoteAnonServerFactory()],
    )


@pytest.fixture()
def an_empty_pending_stage(
        some_streams, an_idis_connection, tmpdir, a_records_db
) -> PendingAnon:
    """An empty pending stage with a mocked connection to IDIS and mocked records db
    """
    return PendingAnon(
        name="pending",
        path=Path(tmpdir) / "pending_anon",
        streams=some_streams,
        idis_connection=an_idis_connection,
        records=a_records_db,
    )


@pytest.fixture
def a_records_db() -> IDISSendRecords:
    """An initialised empty records database"""
    return IDISSendRecords(get_memory_only_sessionmaker())


@pytest.fixture
def a_pending_anon_stage_with_data(
        an_empty_pending_stage, some_studies) -> PendingAnon:
    """A pending stage to which three studies have been pushed"""
    for study in some_studies:
        an_empty_pending_stage.push_study(study)
    return an_empty_pending_stage


def test_idis_connection(an_idis_connection):
    an_idis_connection.servers = [RemoteAnonServerFactory(name='server1'),
                                  RemoteAnonServerFactory(name='server2')]
    assert an_idis_connection.get_server('server1')
    with pytest.raises(UnknownServerException):
        an_idis_connection.get_server('unknown server')


def test_pending_anon_push(an_empty_pending_stage, mock_anon_client_tool, a_study):
    """Pending should create IDIS jobs when studies are pushed to it
     """
    # make sure initial state is as expected:
    assert len(mock_anon_client_tool.mock_calls) == 0  # No calls to IDIS
    with an_empty_pending_stage.records.get_session() as session:
        assert len(session.get_all()) == 0  # No records

        # push some data to stage
        an_empty_pending_stage.push_study(a_study)

        # A job should have been made with IDIS
        assert mock_anon_client_tool.create_path_job.called

        # A record of this should have been made
        assert len(session.get_all()) == 1


def test_pending_anon_check_status(mock_anon_client_tool,
                                   a_pending_anon_stage_with_data):
    """A pending stage should be able to check on job status with IDIS
    """

    stage = a_pending_anon_stage_with_data

    with stage.records.get_session() as session:
        # make sure two different servers were used for creating jobs
        session.get_all()[0].server_name = stage.idis_connection.servers[0].name
        session.get_all()[1].server_name = stage.idis_connection.servers[1].name

    # get record for each stage
    studies = stage.get_all_studies()

    # update all records for each stage with IDIS
    studies = stage.update_records(studies)

    # get important groups: Studies finished, errored, still pending
    finished = [x for x in studies if x.status == JobStatus.DONE]
    errored = [x for x in studies if x.status == JobStatus.ERROR]
    still_going = [x for x in studies if x.status == JobStatus.ACTIVE]
    cancelled = [x for x in studies if x.status == JobStatus.INACTIVE]

    test = 1


def test_push_study(an_incoming_stage, some_stages):
    """Basic transfer of studies between stages
    """
    incoming = an_incoming_stage
    stage1 = some_stages[0]

    assert len(incoming.get_all_studies()) == 3
    assert len(stage1.get_all_studies()) == 0

    stage1.push_study(incoming.get_all_studies()[0])
    assert len(incoming.get_all_studies()) == 2
    assert len(stage1.get_all_studies()) == 1

    stage1.push_study(incoming.get_all_studies()[0])
    assert len(incoming.get_all_studies()) == 1
    assert len(stage1.get_all_studies()) == 2


def test_push_study_exception_missing_stream(a_stage, a_study):
    """Stream does not exist in target stage
    """
    a_stage.streams.remove(a_study.stream)
    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)


def test_push_study_exceptions(a_stage, a_study):
    """ Data does not exist for study (unexpected but not impossible)
    """
    a_study.path.rename(a_study.path.parent / "removed")  # now study has no data
    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)


def test_push_study_callback_fail(a_stage, a_study):
    """push callback fails for some reason on target stage
    """

    assert len(a_study.stage.get_all_studies()) == 3
    assert len(a_stage.get_all_studies()) == 0

    a_stage.push_study_callback = Mock(
        side_effect=PushStudyCallbackException("Something really went wrong here")
    )

    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)

    # study move should have been rolled back
    assert len(a_study.stage.get_all_studies()) == 3
    assert len(a_stage.get_all_studies()) == 0


def test_push_study_out_of_space(a_stage, a_study, monkeypatch):
    """out of space on target stage
    """
    monkeypatch.setattr(
        "idissend.core.shutil.move", Mock(side_effect=IOError("out of space"))
    )

    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)
