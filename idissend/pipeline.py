""" core and stages define the pieces of a pipeline,
this module has classes and methods for the relationships between them
"""


import logging

from anonapi.client import AnonClientTool
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMapping, UNCMap, UNCPath
from anonapi.responses import JobStatus
from collections import Counter
from idissend.core import Stream, Person, Stage
from idissend.persistence import IDISSendRecords, get_db_sessionmaker
from idissend.stages import CoolDown, PendingAnon, IDISConnection, Trash
from pathlib import Path

class DefaultPipeline:
    """Standard version of a idissend pipeline: data comes in, is exposed to IDIS for
    anonymization, then ends up in completed. There are trash and errorred stages to
    hold studies if needed

    Responsibilities
    ----------------
    Pipeline does:
    * Inspect status and studies of stages
    * Push studies between stages if needed
    * log errors raised by stages, but not necessarily catch

    Pipeline does NOT:
    * Handle any actual files (this is up to the stage)
    * Handle any rollback on error (this is also up to the stage)

    """

    def __init__(
        self,
        incoming: CoolDown,
        pending: PendingAnon,
        finished: CoolDown,
        trash: Trash,
        errored: Stage,
    ):
        """

        Parameters
        ----------
        incoming: CoolDown
            Data comes in here
        pending: PendingAnon
            Data waits here to be downloaded and anonymized by IDIS
        finished: CoolDown
            When IDIS is done, move the data from pending here
        trash: Trash
            Any study here can be removed when needed
        errored:
            Holds studies with errors, either in IDIS or in pipeline itself
        """
        self.incoming = incoming
        self.pending = pending
        self.finished = finished
        self.trash = trash
        self.errored = errored
        self.all_stages = [incoming, pending, finished, trash, errored]
        self.logger = logging.getLogger(f"pipeline {id(self)}")

    def run_once(self):
        """Check each stage and perform actions one time

        Raises
        ------
        IDISSendException:
            If anything goes wrong executing the run that the pipeline cannot handle
            by itself
        """

        self.logger.info("Running once")

        studies = self.pending.get_all_studies()
        self.logger.debug(f"Updating IDIS status for {len(studies)} pending jobs")
        self.pending.update_records(studies)
        self.logger.debug(
            f"Found {str(dict(Counter([x.last_status for x in studies])))}."
            f" Taking action based on status"
        )

        self.finished.push_studies(
            [x for x in studies if x.last_status == JobStatus.DONE]
        )
        self.trash.push_studies(
            [x for x in studies if x.last_status == JobStatus.INACTIVE]
        )
        self.errored.push_studies(
            [x for x in studies if x.last_status == JobStatus.ERROR]
        )

        self.logger.debug("Checking for new studies coming in")
        cooled_down = self.incoming.get_all_cooled_studies()
        self.logger.debug(f"Found {len(cooled_down)}. Pushing to pending")
        self.pending.push_studies(cooled_down)

        self.logger.debug("empty old trash")
        self.trash.delete_all()

        self.logger.debug(
            f"Moving finished studies older than "
            f"{self.finished.cool_down} minutes to trash"
        )
        self.trash.push_studies(self.finished.get_all_cooled_studies())

    def get_status(self) -> str:
        """Status for all stages"""
        status_lines = []
        for stage in self.all_stages:
            studies = stage.get_all_studies()
            status_lines.append(
                f"{stage.name} contains {len(studies)} "
                f"studies: {[str(x) for x in studies]}"
            )

        return "\n".join(status_lines)
