#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `idissend` package."""
from pathlib import Path

import pytest
import shutil

from idissend import idissend
from idissend.idissend import IncomingFolder, Stream, Person
from tests import RESOURCE_PATH


@pytest.fixture
def an_input_folder(tmpdir) -> Path:
    """A folder with some DICOM files in <stream>/<study> structure"""

    copy_of_folder = Path(str(tmpdir)) / "an_input_folder"
    shutil.copytree(RESOURCE_PATH / "an_input_folder", copy_of_folder)
    return copy_of_folder


@pytest.fixture
def some_streams():
    """Some streams, some of which have some data in an_input_folder()"""
    jessie = Person("Jessie Admin", email="j.admin@localhost.com")
    rachel = Person("Rachel Search", email="r.search@localhost.com")
    jack = Person("Jack Serious", email="j.serious@localhost.com")
    return [
        Stream(
            name="project1",
            output_folder=Path("test_output_project1"),
            idis_project="Wetenschap-Algemeen",
            pims_key="1234",
            contact=jessie,
        ),
        Stream(
            name="project2",
            output_folder=Path("test_output_project2"),
            idis_project="Wetenschap-Algemeen",
            pims_key="4444",
            contact=rachel,
        ),
        Stream(
            name="project3",
            output_folder=Path("test_output_project3"),
            idis_project="Wetenschap-Algemeen",
            pims_key="5555",
            contact=jack,
        ),
    ]


def test_incoming_folder(an_input_folder, some_streams):
    folder = IncomingFolder(path=an_input_folder, streams=some_streams)
    studies = folder.get_studies()

    assert len(studies) == 3
    assert 'project1' in [x.stream.name for x in studies]
    assert 'project2' in [x.stream.name for x in studies]


# TODO test cooldown, implement pending anon, implement trash, implement control
