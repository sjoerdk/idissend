"""Conftest.py is loaded for each pytest.
Contains fixtures shared by multiple tests.
"""
import logging
import shutil
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from anonapi.responses import JobStatus
from anonapi.testresources import (
    JobInfoFactory,
    MockAnonClientTool,
    RemoteAnonServerFactory,
)

from idissend.core import Stage, Stream, Study
from idissend.persistence import IDISSendRecords, get_memory_only_sessionmaker
from idissend.pipeline import IDISPipeline
from idissend.stages import CoolDown, IDISConnection, PendingAnon, Trash
from tests import RESOURCE_PATH
from tests.factories import StreamFactory


@pytest.fixture
def some_studies(an_incoming_stage) -> List[Study]:
    """Some studies in the incoming stage with some actual data on disk."""
    return an_incoming_stage.get_all_studies()


@pytest.fixture
def a_study(some_studies) -> Study:
    """A study in the incoming stage with some actual data on disk."""
    return some_studies[0]


@pytest.fixture
def an_idssend_structured_folder(tmpdir) -> Path:
    """A folder with some DICOM files in <stream>/<study> structure"""

    copy_of_folder = Path(str(tmpdir)) / "an_idssend_structured_folder"
    shutil.copytree(RESOURCE_PATH / "an_idssend_structured_folder", copy_of_folder)
    return copy_of_folder


@pytest.fixture
def some_streams() -> List[Stream]:
    """Some streams, some of which have some data in an_idssend_structured_folder()"""
    return [
        StreamFactory(name="project1"),
        StreamFactory(name="project2"),
        StreamFactory(name="project3"),
    ]


@pytest.fixture
def some_stages(some_streams, tmpdir) -> List[Stage]:
    """Two stages that each have some streams and an empty tmp path"""

    return [
        Stage(name="stage1", streams=some_streams, path=Path(tmpdir) / "stage1"),
        Stage(name="stage2", streams=some_streams, path=Path(tmpdir) / "stage2"),
    ]


@pytest.fixture
def a_stage(some_stages):
    """A stage with some streams and an empty tmp path"""
    return some_stages[0]


@pytest.fixture
def an_incoming_stage(an_idssend_structured_folder, some_streams) -> CoolDown:
    """An incoming stage which has some actual content on disk"""
    return CoolDown(
        name="incoming",
        path=an_idssend_structured_folder,
        streams=some_streams,
        cool_down=0,
    )


@pytest.fixture()
def an_empty_pending_stage(
    some_streams, an_idis_connection, tmpdir, a_records_db
) -> PendingAnon:
    """An empty pending stage with a mocked connection to IDIS and mocked
    records db
    """
    return PendingAnon(
        name="pending",
        path=Path(tmpdir) / "pending_anon",
        streams=some_streams,
        idis_connection=an_idis_connection,
        records=a_records_db,
    )


@pytest.fixture
def an_idis_connection(mock_anon_client_tool):
    """An idis connection that mocks repsonses and does not hit any server"""
    return IDISConnection(
        client_tool=mock_anon_client_tool,
        servers=[RemoteAnonServerFactory(), RemoteAnonServerFactory()],
    )


@pytest.fixture
def a_records_db() -> IDISSendRecords:
    """An initialised empty records database"""
    return IDISSendRecords(get_memory_only_sessionmaker())


@pytest.fixture
def mock_anon_client_tool(monkeypatch):
    """An anonymization API client tool that does not hit the server but returns
    some example responses instead. Also records calls
    """

    # force sequence to be able to be able to assert job ids in tests
    some_responses = [
        JobInfoFactory(status=JobStatus.DONE, __sequence=0),
        JobInfoFactory(status=JobStatus.ERROR, __sequence=1),
        JobInfoFactory(status=JobStatus.INACTIVE, __sequence=2),
    ]
    mock = Mock(wraps=MockAnonClientTool(responses=some_responses))
    # set reset to avoid Mock.wraps triggering NotImplemented()
    mock.reset_job = lambda server, job_id: f"Mock reset {job_id}"
    return mock


@pytest.fixture
def a_pipeline(
    an_incoming_stage, an_empty_pending_stage, an_idis_connection, tmp_path, caplog
):
    """A default pipeline with all-mocked connections to outside servers.
    Integration test light. Useful for checking log messages etc.
    """
    # capture all logs
    caplog.set_level(logging.DEBUG)

    # make sure all stages have the same streams
    streams = an_incoming_stage.streams
    an_empty_pending_stage.streams = streams
    cooled_down = Stage(
        name="cooled_down", path=Path(tmp_path) / "cooled_down", streams=streams
    )
    finished = CoolDown(
        name="finished", path=Path(tmp_path) / "finished", streams=streams, cool_down=0
    )
    trash = Trash(name="trash", path=Path(tmp_path) / "trash", streams=streams)
    errored = Stage(name="errored", path=Path(tmp_path) / "errored", streams=streams)

    return IDISPipeline(
        incoming=an_incoming_stage,
        cooled_down=cooled_down,
        pending=an_empty_pending_stage,
        finished=finished,
        trash=trash,
        errored=errored,
    )
