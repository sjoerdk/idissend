"""Conftest.py is loaded for each pytest. Contains fixtures shared by multiple tests
"""
import shutil
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from anonapi.responses import JobStatus
from anonapi.testresources import (
    RemoteAnonServerFactory,
    JobInfoFactory,
    MockAnonClientTool,
)

from idissend.core import Stream, Stage, Study
from idissend.persistence import IDISSendRecords, get_memory_only_sessionmaker
from idissend.stages import CoolDown, PendingAnon, IDISConnection
from tests import RESOURCE_PATH
from tests.factories import StreamFactory


@pytest.fixture
def some_studies(an_incoming_stage) -> List[Study]:
    """some studies in the incoming stage with some actual data on disk"""
    return an_incoming_stage.get_all_studies()


@pytest.fixture
def a_study(some_studies) -> Study:
    """A study in the incoming stage with some actual data on disk"""
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
    some example responses instead. Also records calls"""
    some_responses = [
        JobInfoFactory(status=JobStatus.DONE),
        JobInfoFactory(status=JobStatus.ERROR),
        JobInfoFactory(status=JobStatus.INACTIVE),
    ]
    # mock wrapper to be able to record responses
    return Mock(wraps=MockAnonClientTool(responses=some_responses))
