#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `idissend` package."""
from itertools import cycle
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
import shutil

from anonapi.testresources import (
    JobInfoFactory,
    JobStatus,
    MockAnonClientTool,
    RemoteAnonServerFactory,
)

from idissend.core import (Stream,
    Study, Stage,
    StudyPushException,
    PushStudyCallbackException, )
from idissend.stages import Incoming, IDISConnection, PendingAnon
from idissend.persistence import IDISSendRecords, get_session, get_memory_only_session
from tests import RESOURCE_PATH
from tests.factories import MockIncomingFileFactory, StreamFactory, StudyFactory


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
def an_incoming_stage(an_idssend_structured_folder, some_streams) -> Incoming:
    """An incoming stage which has some actual content on disk"""
    return Incoming(
        name="incoming", path=an_idssend_structured_folder, streams=some_streams
    )


@pytest.fixture
def a_study(an_incoming_stage) -> Study:
    """A study in the incoming stage with some actual data on disk"""
    return an_incoming_stage.get_all_studies()[0]


@pytest.fixture
def a_records_db() -> IDISSendRecords:
    """An initialised empty records database"""
    return IDISSendRecords(get_memory_only_session())


def test_incoming_folder(an_incoming_stage):
    folder = an_incoming_stage
    studies = folder.get_all_studies()

    assert len(studies) == 3
    assert "project1" in [x.stream.name for x in studies]
    assert "project2" in [x.stream.name for x in studies]


def test_cooldown(monkeypatch):
    """Studies are considered complete after a cooldown period. Does this work?"""
    # a study with some files
    some_files = [Path(), Path(), Path()]
    study: Study = StudyFactory()
    study.get_files = lambda: some_files  # don't check path on disk, just mock

    # checking the age of these files will yield 10, 11, 12
    monkeypatch.setattr("idissend.core.IncomingFile.age",
                        Mock(side_effect=cycle([10, 11, 12])))

    assert study.is_older_than(5)
    assert not study.is_older_than(11)
    assert not study.is_older_than(15)


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
def a_pending_anon_stage(
    some_streams, an_idis_connection, tmpdir, a_records_db
) -> PendingAnon:
    """An empty pending stage with a mocked connection to IDIS
    """
    return PendingAnon(
        name="pending",
        path=Path(tmpdir) / "pending_anon",
        streams=some_streams,
        idis_connection=an_idis_connection,
        records=a_records_db,
    )


def test_pending_anon(a_pending_anon_stage, mock_anon_client_tool, a_study):
    """Pending should:
     * send studies to IDIS
     * check IDIS job progress
     * send finished studies on
     * deal with errored or stuck IDIS jobs

    Check that this happens
     """
    # make sure initial state is as expected:
    assert len(mock_anon_client_tool.mock_calls) == 0  # No calls to IDIS
    assert len(a_pending_anon_stage.records.get_all()) == 0  # No records

    # push some data to stage
    a_pending_anon_stage.push_study(a_study)

    # A job should have been made with IDIS
    assert mock_anon_client_tool.create_path_job.called

    # A record of this should have been made
    assert len(a_pending_anon_stage.records.get_all()) == 1

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
