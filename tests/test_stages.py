import logging
from pathlib import Path
from unittest.mock import Mock

import pytest
from anonapi.client import ClientToolException
from anonapi.paths import UNCPath, UNCMapping, UNCMap
from anonapi.responses import JobsInfoList
from anonapi.testresources import (
    RemoteAnonServerFactory,
    JobInfoFactory,
    JobStatus,
)

from idissend.core import StudyPushException
from idissend.stages import (
    PendingAnon,
    UnknownServerException,
    IDISCommunicationException,
    RecordNotFoundException,
    Trash,
)
from tests.factories import StreamFactory


@pytest.fixture
def a_pending_anon_stage_with_data(an_empty_pending_stage, some_studies) -> PendingAnon:
    """A pending stage to which three studies have been pushed"""
    for study in some_studies:
        an_empty_pending_stage.push_study(study)
    return an_empty_pending_stage


@pytest.fixture
def a_trash_stage(a_pending_anon_stage_with_data, tmpdir) -> Trash:
    """A trash stage with temp path on disk and the same streams as
    a_pending_anon_stage_with_data
    """

    trash = Trash(
        name="Trash",
        streams=a_pending_anon_stage_with_data.streams,
        path=Path(tmpdir) / "trash",
    )
    trash.assert_all_paths()
    return trash


def test_idis_connection(an_idis_connection):
    an_idis_connection.servers = [
        RemoteAnonServerFactory(name="server1"),
        RemoteAnonServerFactory(name="server2"),
    ]
    assert an_idis_connection.get_server("server1")
    with pytest.raises(UnknownServerException):
        an_idis_connection.get_server("unknown server")


def test_pending_anon_push(an_empty_pending_stage, mock_anon_client_tool, a_study):
    """Pending should create IDIS jobs when studies are pushed to it"""
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


def test_pending_anon_push_unc_paths(
    an_empty_pending_stage, mock_anon_client_tool, a_study
):
    """Created jobs should have UNC input and output. Otherwise IDIS will just
    choke on them (design flaw definitely)
    """

    # study with local source and destination. As is these paths make no sense
    # on an IDIS server

    a_stream = StreamFactory(output_folder=Path("/mnt/datashare/some/folder"))
    an_empty_pending_stage.streams.append(a_stream)

    # map local paths to UNC paths to make translation possible
    mapping = UNCMapping(
        maps=[
            UNCMap(local=Path("/"), unc=UNCPath(r"\\server\share")),
            UNCMap(
                local=Path("/mnt/datashare"), unc=UNCPath(r"\\dataserver\datashare")
            ),
        ]
    )
    an_empty_pending_stage.unc_mapping = mapping
    an_empty_pending_stage.push_study(a_study, a_stream)

    # A job should have been made with IDIS
    assert mock_anon_client_tool.create_path_job.called

    # and the paths should be UNC
    idis_source_path = mock_anon_client_tool.create_path_job.call_args[1]["source_path"]
    idis_destination_path = mock_anon_client_tool.create_path_job.call_args[1][
        "destination_path"
    ]

    assert UNCPath.is_unc(idis_source_path)
    assert UNCPath.is_unc(idis_destination_path)


def test_pending_anon_push_non_unc_paths(
    an_empty_pending_stage, mock_anon_client_tool, a_study
):
    """If a path is set that cannot be translated the push should fail"""

    a_stream = StreamFactory(output_folder=Path(r"C:\data"))
    an_empty_pending_stage.streams.append(a_stream)
    an_empty_pending_stage.unc_mapping = UNCMapping(maps=[])  # triggers checking
    with pytest.raises(StudyPushException) as e:
        an_empty_pending_stage.push_study(a_study, a_stream)
    assert "could not be mapped" in str(e)


def test_pending_anon_push_idis_exception(
    an_empty_pending_stage, mock_anon_client_tool, a_study
):
    """Pending should create IDIS jobs. What happens when these fail?"""
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
    """A pending stage should be able to check on job status with IDIS"""

    stage = a_pending_anon_stage_with_data

    with stage.records.get_session() as session:
        # make sure two different servers were used for creating jobs
        session.get_all()[0].server_name = stage.idis_connection.servers[0].name
        session.get_all()[1].server_name = stage.idis_connection.servers[1].name

    # get record for each stage
    studies = stage.get_all_studies()

    # contact IDIS to get the latest on the jobs corresponding to each study
    stage.update_records(studies)
    records = stage.get_records(studies)

    # get important groups: Studies finished, errored, still pending
    finished = [x for x in records if x.last_status == JobStatus.DONE]
    errored = [x for x in records if x.last_status == JobStatus.ERROR]
    still_going = [x for x in records if x.last_status == JobStatus.ACTIVE]
    cancelled = [x for x in records if x.last_status == JobStatus.INACTIVE]

    assert len(finished) == 1
    assert len(errored) == 1
    assert len(cancelled) == 1
    assert len(still_going) == 0


def test_pending_anon_check_status_exceptions(
    mock_anon_client_tool, a_pending_anon_stage_with_data
):
    """Handle errors in interaction with IDIS gracefully"""

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


def test_pending_anon_missing_record(
    a_pending_anon_stage_with_data, a_trash_stage, mock_anon_client_tool, a_records_db
):
    """Pending stage should be able to handle missing records"""
    # This stage has 3 studies already pushed to it. Verify studies,
    # IDIS jobs and records
    pending = a_pending_anon_stage_with_data
    assert len(pending.get_all_studies()) == 3
    with a_records_db.get_session() as session:
        assert len(session.get_all()) == 3

    # now something bad happens for some reason. One record is lost!
    with a_records_db.get_session() as session:
        session.delete(session.get_all()[0])

    # getting all records will fail
    with pytest.raises(RecordNotFoundException):
        pending.get_records(pending.get_all_studies())

    # however you can obtain the studies that cause the exception and remove them
    orphaned = pending.get_all_orphaned_studies()
    a_trash_stage.push_studies(orphaned)

    # now the remaining studies can be obtained again
    assert len(pending.get_all_studies()) == 2


def test_pending_anon_reset_if_existing(
    mock_anon_client_tool, an_empty_pending_stage, a_study, a_stage
):
    """When an IDIS job has been created for a study before, reset that job"""

    pending = an_empty_pending_stage
    # to start, no records have been created yet
    assert pending.get_all_records() == []

    # pushing a study should create a record
    pending.push_studies([a_study])
    assert len(pending.get_all_records()) == 1

    # study is moved to a different stage
    a_stage.push_studies(pending.get_all_studies()[:1])

    # when study is pushed back,
    pending.push_studies(a_stage.get_all_studies()[:1])
    # no new record should have been created
    assert len(pending.get_all_records()) == 1


def test_trash_stage(a_pending_anon_stage_with_data, a_trash_stage, caplog):
    """Emptying trash should work and be logged"""
    caplog.set_level(logging.DEBUG)

    a_trash_stage.push_studies(a_pending_anon_stage_with_data.get_all_studies())
    assert len(a_trash_stage.get_all_studies()) == 3

    a_trash_stage.delete_all()
    assert len(a_trash_stage.get_all_studies()) == 0
    assert "Removing data for 3 studies" in caplog.text
