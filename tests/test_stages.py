from pathlib import Path
from unittest.mock import Mock

import pytest
from anonapi.client import ClientToolException
from anonapi.responses import JobsInfoList
from anonapi.testresources import (
    RemoteAnonServerFactory,
    JobInfoFactory,
    JobStatus,
    MockAnonClientTool,
)

from idissend.core import StudyPushException
from idissend.persistence import IDISSendRecords, get_memory_only_sessionmaker
from idissend.stages import (
    PendingAnon,
    IDISConnection,
    UnknownServerException,
    IDISCommunicationException,
)


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
def a_pending_anon_stage_with_data(an_empty_pending_stage, some_studies) -> PendingAnon:
    """A pending stage to which three studies have been pushed"""
    for study in some_studies:
        an_empty_pending_stage.push_study(study)
    return an_empty_pending_stage


def test_idis_connection(an_idis_connection):
    an_idis_connection.servers = [
        RemoteAnonServerFactory(name="server1"),
        RemoteAnonServerFactory(name="server2"),
    ]
    assert an_idis_connection.get_server("server1")
    with pytest.raises(UnknownServerException):
        an_idis_connection.get_server("unknown server")


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


def test_pending_anon_push_idis_exception(
    an_empty_pending_stage, mock_anon_client_tool, a_study
):
    """Pending should create IDIS jobs. What happens when these fail?
     """
    # contact IDIS will not work
    mock_anon_client_tool.create_path_job = Mock(
        side_effect=ClientToolException("Terrible API error")
    )

    # pushing should raise
    with pytest.raises(StudyPushException):
        an_empty_pending_stage.push_study(a_study)

    # any copy or move should have been rolled back
    assert len(an_empty_pending_stage.get_all_studies()) == 0


def test_pending_anon_check_status(
    mock_anon_client_tool, a_pending_anon_stage_with_data
):
    """A pending stage should be able to check on job status with IDIS
    """

    stage = a_pending_anon_stage_with_data

    with stage.records.get_session() as session:
        # make sure two different servers were used for creating jobs
        session.get_all()[0].server_name = stage.idis_connection.servers[0].name
        session.get_all()[1].server_name = stage.idis_connection.servers[1].name

    # get record for each stage
    studies = stage.get_all_studies()

    # contact IDIS to get the latest on the jobs corresponding to each study
    studies = stage.update_records(studies)

    # get important groups: Studies finished, errored, still pending
    finished = [x for x in studies if x.last_status == JobStatus.DONE]
    errored = [x for x in studies if x.last_status == JobStatus.ERROR]
    still_going = [x for x in studies if x.last_status == JobStatus.ACTIVE]
    cancelled = [x for x in studies if x.last_status == JobStatus.INACTIVE]

    assert len(finished) == 1
    assert len(errored) == 1
    assert len(cancelled) == 1
    assert len(still_going) == 0


def test_pending_anon_check_status_exceptions(
    mock_anon_client_tool, a_pending_anon_stage_with_data
):
    """Handle errors in interaction with IDIS gracefully

    """

    stage = a_pending_anon_stage_with_data
    studies = stage.get_all_studies()

    # Contacting IDIS will not work at all (for example when server is down)
    mock_anon_client_tool.get_job_info_list = Mock(
        side_effect=ClientToolException("Terrible API error")
    )

    with pytest.raises(IDISCommunicationException):
        stage.update_records(studies)

    # Contacting IDIS will work, but not all job ids are found (only id=1 is returned)
    mock_anon_client_tool.get_job_info_list = Mock(
        return_value=JobsInfoList(job_infos=[JobInfoFactory(job_id=1)])
    )

    with pytest.raises(IDISCommunicationException):
        stage.update_records(studies)


