"""
Integration test of a pipeline consisting of different streams and stages.
For testing log messages etc.
"""
from unittest.mock import Mock

import pytest
from anonapi.client import ClientToolException

from idissend.exceptions import IDISSendException
from idissend.stages import IDISCommunicationException


def test_pipline_regular_operation(a_pipeline, caplog):
    """Check logs for regular operation"""
    a_pipeline.run_once()
    a_pipeline.run_once()

    # this is just meant for visual inspection to develop logs. No real tests here
    assert "Running once" in caplog.text


def test_pipeline_idis_exceptions(a_pipeline, caplog, an_idis_connection):
    """What happens when IDIS connection fails"""

    an_idis_connection.client_tool.get_job_info_list = Mock(
        side_effect=ClientToolException(
            "IDIS fell over. Out of the window. Into a pond. "
            "Full of sharks. Radioactive Sharks. Connection lost"
        )
    )
    a_pipeline.run_once()  # import from incoming to pending
    status_before = a_pipeline.get_status()
    with pytest.raises(IDISSendException) as e:
        a_pipeline.run_once()  # IDIS call from pending will raise exception

    assert e.type == IDISCommunicationException
    assert status_before == a_pipeline.get_status()  # nothing should have changed


def test_pipeline_record_not_found_exception(a_pipeline, caplog, a_records_db):
    """What happens when a single record is not found. This can occur after errors,
    or data for studies is somehow moved into the streams outside idissend
    """

    # push to pending and create IDIS jobs
    a_pipeline.run_once()  # import from incoming to pending

    # now remove a record
    with a_records_db.get_session() as session:
        session.delete(session.get_all()[0])

    # running again will fail in the pending stage because one study now has no
    # record, so it is unknown which IDIS job has been made for it
    # with pytest.raises(RecordNotFoundException):
    a_pipeline.run_once()  # import from incoming to pending

    # now record with exception should have been moved to errored
    assert len(a_pipeline.errored.get_all_studies()) == 1
