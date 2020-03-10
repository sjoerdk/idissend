"""
Integration test of a pipeline consisting of different streams and stages.
For testing log messages etc.
"""
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest
from anonapi.client import ClientToolException
from sqlalchemy.exc import SQLAlchemyError

from idissend.core import Stage
from idissend.persistence import IDISSendRecordsSession
from idissend.exceptions import IDISSendException
from idissend.pipeline import DefaultPipeline
from idissend.stages import Trash, IDISCommunicationException


@pytest.fixture
def a_pipeline(an_incoming_stage, an_empty_pending_stage,
               an_idis_connection, tmp_path, caplog):
    """A default pipeline with all-mocked connections to outside servers.
    Integration test light. Useful for checking log messages etc."""
    # capture all logs
    caplog.set_level(logging.DEBUG)

    # make sure all stages have the same streams
    streams = an_incoming_stage.streams
    an_empty_pending_stage.streams = streams
    finished = Stage(name='finished', path=Path(tmp_path)/'finished', streams=streams)
    trash = Trash(name="Trash", path=Path(tmp_path)/'trash', streams=streams)
    errored = Stage(name='errored', path=Path(tmp_path) / 'errored',
                    streams=streams)

    return DefaultPipeline(incoming=an_incoming_stage,
                           pending=an_empty_pending_stage,
                           finished=finished,
                           trash=trash, errored=errored)


def test_pipline_regular_operation(a_pipeline, caplog):
    """ check logs for regular operation """
    a_pipeline.run_once()
    a_pipeline.run_once()

    # this is just meant for visual inspection to develop logs. No real tests here
    assert "Running once" in caplog.text


def test_pipline_IDIS_exceptions(a_pipeline, caplog, an_idis_connection):
    """What happens when IDIS connection fails"""

    an_idis_connection.client_tool.get_job_info_list = Mock(
        side_effect=ClientToolException(
            "IDIS fell over. Out of the window. Into a pond. "
            "Full of sharks. Radioactive Sharks. Connection lost")
    )
    a_pipeline.run_once()   # import from incoming to pending
    status_before = a_pipeline.get_status()
    with pytest.raises(IDISSendException) as e:
        a_pipeline.run_once()   # IDIS call from pending will raise exception

    assert e.type == IDISCommunicationException
    assert status_before == a_pipeline.get_status()  # nothing should have changed


def test_pipline_records_exceptions(a_pipeline, caplog, monkeypatch):
    """What happens when Records db connection fails"""

    # writing to records db will raise exception
    monkeypatch.setattr('idissend.persistence.IDISSendRecordsSession.add',
                        Mock(side_effect=SQLAlchemyError('Database has headache')))

    status_before = a_pipeline.get_status()
    with pytest.raises(IDISSendException) as e:
        a_pipeline.run_once()   # import from incoming to pending

    assert "Rolling back" in caplog.text
    assert status_before == a_pipeline.get_status()  # nothing should have changed
