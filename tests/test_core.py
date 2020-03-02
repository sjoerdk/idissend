from idissend.core import Study
from itertools import cycle
from pathlib import Path
from tests.factories import StudyFactory
from unittest.mock import Mock


def test_incoming_folder(an_incoming_stage):
    """Basic functions for an incoming folder"""
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

    # checking the age of these files will yield 10, 11, 12, 10, etc..
    monkeypatch.setattr(
        "idissend.core.IncomingFile.age", Mock(side_effect=cycle([10, 11, 12]))
    )

    assert study.is_older_than(5)
    assert not study.is_older_than(11)
    assert not study.is_older_than(15)
