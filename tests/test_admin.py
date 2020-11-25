import pytest

from idissend.admin import Admin, IDISAdmin
from idissend.pipeline import ObjectNotFound


@pytest.fixture
def an_admin(a_pipeline) -> Admin:
    return Admin(pipeline=a_pipeline)


@pytest.fixture
def an_idis_admin(a_pipeline) -> IDISAdmin:
    return IDISAdmin(pipeline=a_pipeline)


def test_admin_status(an_admin, caplog):
    assert "incoming contains 3 studies" in an_admin.status()


def test_admin_list_studies(an_admin, caplog):
    assert "project1:series2" in an_admin.list_studies(stage="incoming")
    assert "project1" not in an_admin.list_studies(stage="incoming", ids_only=True)
    assert "series1" in an_admin.list_studies(stage="incoming", ids_only=True)


def test_admin_list_studies_exception(an_admin, caplog):
    with pytest.raises(ObjectNotFound):
        an_admin.list_studies(stage="unknown_stage")


def test_admin_move_studies(a_pipeline, an_idis_admin, caplog):
    """Moving around studies by id"""
    assert len(a_pipeline.incoming.get_all_studies()) == 3
    assert len(a_pipeline.trash.get_all_studies()) == 0

    ids = [x.study_id for x in a_pipeline.incoming.get_all_studies()]
    an_idis_admin.move_studies(ids=ids[0:2], to_stage="trash")

    assert len(a_pipeline.incoming.get_all_studies()) == 1
    assert len(a_pipeline.trash.get_all_studies()) == 2


def test_admin_move_unknown_study(a_pipeline, an_idis_admin, caplog):
    ids = [x.study_id for x in a_pipeline.incoming.get_all_studies()]
    # moving 1 existing and one unknown study should fail

    with pytest.raises(ObjectNotFound):
        an_idis_admin.move_studies(ids=ids[0:1] + ["UNKNOWN"], to_stage="trash")

    # and leave all unmoved
    assert len(a_pipeline.incoming.get_all_studies()) == 3


def test_admin_move_unknown_stage(a_pipeline, an_idis_admin, caplog):
    """Moving to unknown stage should leave all unmoved"""
    ids = [x.study_id for x in a_pipeline.incoming.get_all_studies()]
    with pytest.raises(ObjectNotFound):
        an_idis_admin.move_studies(ids=ids, to_stage="unknown_stage")

    assert len(a_pipeline.incoming.get_all_studies()) == 3


def test_default_admin(a_pipeline, an_idis_admin, mock_anon_client_tool, caplog):

    a_pipeline.run_once()  # make sure some jobs have been (mock) sent to idis
    some_ids = [x.study_id for x in a_pipeline.pending.get_all_studies()][0:2]
    assert an_idis_admin.get_job_ids(some_ids) == ["0", "1"]
    assert "series1" in "".join(an_idis_admin.get_error_messages(some_ids))
