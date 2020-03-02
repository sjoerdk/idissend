"""Conftest.py is loaded for each pytest. Contains fixtures shared by multiple tests
"""
import shutil
from pathlib import Path
from typing import List

import pytest

from idissend.core import Stream, Stage, Study
from idissend.stages import Incoming
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
def an_incoming_stage(an_idssend_structured_folder, some_streams) -> Incoming:
    """An incoming stage which has some actual content on disk"""
    return Incoming(
        name="incoming", path=an_idssend_structured_folder, streams=some_streams
    )
