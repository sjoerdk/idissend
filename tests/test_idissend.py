#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `idissend` package."""
from pathlib import Path
from typing import List

import pytest
import shutil

from idissend import idissend
from idissend.idissend import Incoming, Stream, Person, AgedPath, Study
from tests import RESOURCE_PATH
from tests.factories import MockAgedPathFactory, StreamFactory, StudyFactory


@pytest.fixture
def an_idssend_structured_folder(tmpdir) -> Path:
    """A folder with some DICOM files in <stream>/<study> structure"""

    copy_of_folder = Path(str(tmpdir)) / "an_idssend_structured_folder"
    shutil.copytree(RESOURCE_PATH / "an_idssend_structured_folder", copy_of_folder)
    return copy_of_folder


@pytest.fixture
def some_streams() -> List[Stream]:
    """Some streams, some of which have some data in an_idssend_structured_folder()"""
    return [StreamFactory(name='project1'),
            StreamFactory(name='project2'),
            StreamFactory(name='project3')]


@pytest.fixture
def an_incoming_folder(an_idssend_structured_folder, some_streams) -> Incoming:
    """An incoming folder which has some actual content on disk"""
    return Incoming(path=an_idssend_structured_folder, streams=some_streams)


def test_incoming_folder(an_incoming_folder):
    folder = an_incoming_folder
    studies = folder.get_studies()

    assert len(studies) == 3
    assert "project1" in [x.stream.name for x in studies]
    assert "project2" in [x.stream.name for x in studies]


def test_cooldown(an_incoming_folder):
    """Studies are considered complete after a cooldown period. Does this work?"""
    some_files = [
        MockAgedPathFactory(age=10),
        MockAgedPathFactory(age=11),
        MockAgedPathFactory(age=12),
    ]
    study: Study = StudyFactory()
    study.get_files = lambda: some_files   # don't check path on disk, just mock

    assert study.is_older_than(5)
    assert not study.is_older_than(11)
    assert not study.is_older_than(15)


def test_pending_anon():
    pass
# TODO test missing files and read errors