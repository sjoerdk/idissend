"""
Integration test of a pipeline consisting of different streams and stages.
For testing log messages etc.
"""
import logging
from pathlib import Path

from idissend.core import Stage
from idissend.pipeline import DefaultPipeline
from idissend.stages import Trash
from tests.factories import StageFactory


def test_pipeline(caplog, an_incoming_stage, an_empty_pending_stage,
                  an_idis_connection, tmp_path):
    # make sure all stages have the same streams
    streams = an_incoming_stage.streams
    an_empty_pending_stage.streams = streams
    finished = Stage(name='finised', path=Path(tmp_path)/'finished', streams=streams)
    trash = Trash(name="Trash", path=Path(tmp_path)/'trash', streams=streams)
    errored = Stage(name='errored', path=Path(tmp_path) / 'errored',
                    streams=streams)

    pipeline = DefaultPipeline(incoming=an_incoming_stage,
                               pending=an_empty_pending_stage,
                               finished=finished,
                               trash=trash, errored=errored)

    caplog.set_level(logging.DEBUG)
    pipeline.run_once()

    # check logs for regular operation

    # check logs when things go wrong
    test = 1
