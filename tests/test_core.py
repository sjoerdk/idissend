import pytest

from idissend.core import Study, StudyPushException, PushStudyCallbackException
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
    """Studies are considered complete after a cool_down period. Does this work?"""
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


def test_push_study(an_incoming_stage, some_stages):
    """Basic transfer of studies between stages"""
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
    """Stream does not exist in target stage"""
    a_stage.streams.remove(a_study.stream)
    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)


def test_push_study_exceptions(a_stage, a_study):
    """Data does not exist for study (unexpected but not impossible)"""
    # remove data for study
    a_study.get_path().rename(a_study.get_path().parent / "removed")
    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)


def test_push_study_to_itself(a_study):
    """Cannot push to a stage a study is in already"""
    with pytest.raises(StudyPushException):
        a_study.stage.push_study(a_study)


def test_push_study_callback_fail(a_stage, a_study):
    """Push callback fails for some reason on target stage"""

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
    """Out of space on target stage"""
    monkeypatch.setattr(
        "idissend.core.shutil.move", Mock(side_effect=IOError("out of space"))
    )

    with pytest.raises(StudyPushException):
        a_stage.push_study(a_study)
