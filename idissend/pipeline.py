"""core and stages define the pieces of a pipeline,
this module has classes and methods for the relationships between them
"""


import logging
from itertools import chain
from typing import Iterator, List

from anonapi.responses import JobStatus
from collections import Counter
from idissend.core import Stage, Study, random_string
from idissend.exceptions import IDISSendException
from idissend.stages import (
    CoolDown,
    PendingAnon,
    IDISStudy,
    RecordNotFoundException,
    Trash,
)


class Pipeline:
    """A collection of linked stages and streams

    This is a base class that does not define any processing logic. To add
    logic on what to do with studies, extend in a Child class

    Responsibilities
    ----------------
    Pipeline does:
    * Inspect stages, check studies in it
    * Push studies between stages if needed
    * log errors raised by stages, but not necessarily catch

    Pipeline does NOT:
    * Handle any actual files (this is up to the stage)
    * Handle any rollback on error (this is also up to the stage)
    """

    def __init__(self, stages: List[Stage]):
        self.stages = stages

    def get_stage(self, name: str) -> Stage:
        """Return first stage in this pipeline that has the given name

        Parameters
        ----------
        name: str
            Name of the stage

        Raises
        ------
        ObjectNotFound
            If no stage by that name is defined
        """
        try:
            return next(stage for stage in self.stages if stage.name == name)
        except StopIteration:
            raise ObjectNotFound(
                f"Stage '{name}' not found in " f"{[x.name for x in self.stages]}"
            )

    def get_study_iter(self) -> Iterator[Study]:
        """Iterates through each study in this pipeline"""
        return chain(*(y.get_all_studies() for y in self.stages))

    def get_studies(self, study_ids: List[str]) -> List[Study]:
        """Find studies with given IDs

        Parameters
        ----------
        study_ids: List[str]
            study_id of each study to find

        Raises
        ------
        ObjectNotFound
            If any study can be found
        """
        to_find = study_ids
        study_iter = self.get_study_iter()
        found = []
        try:
            while to_find:
                study = next(study_iter)
                if study.study_id in to_find:
                    found.append(study)
                    to_find.remove(study.study_id)
        except StopIteration:
            raise ObjectNotFound(f"Studies '{to_find}' not found in pipeline")

        return found

    def get_status(self) -> str:
        """Status for all stages"""
        status_lines = []
        for stage in self.stages:
            studies = stage.get_all_studies()
            status_lines.append(
                f"{stage.name} contains {len(studies)} "
                f"studies: {[str(x) for x in studies]}"
            )

        return "\n".join(status_lines)

    def assert_all_paths(self):
        """Make sure folders for each stage and stream in this pipeline exist"""
        for stage in self.stages:
            stage.assert_all_paths()


class IDISPipeline(Pipeline):
    """Standard version of a idissend pipeline: data comes in, is exposed to IDIS for
    anonymization, then ends up in completed. There are trash and errorred stages to
    hold studies if needed
    """

    def __init__(
        self,
        incoming: CoolDown,
        cooled_down: Stage,
        pending: PendingAnon,
        finished: CoolDown,
        trash: Trash,
        errored: Stage,
    ):
        """

        Parameters
        ----------
        incoming: CoolDown
            Data comes in here. Might contain duplicate ids
        cooled_down: Stage
            Data that has cooled down is renamed to insure unique id and put here
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
        self.cooled_down = cooled_down
        self.pending = pending
        self.finished = finished
        self.trash = trash
        self.errored = errored
        super().__init__(
            stages=[incoming, cooled_down, pending, finished, trash, errored]
        )
        self.logger = logging.getLogger(__name__)

    def run_once(self):
        """Check each stage and perform actions one time

        Raises
        ------
        IDISSendException:
            If anything goes wrong executing the run that the pipeline cannot handle
            by itself
        """

        self.logger.info("Running once")

        studies = self.get_pending_studies()

        self.update_idis_status(studies)

        self.logger.debug("Checking for incoming studies that are now complete")
        cooled_down_studies = self.incoming.get_all_cooled_studies()
        self.logger.debug(
            f"Found {len(cooled_down_studies)}. Pushing to cooled_down"
            f" and renaming to avoid duplicate ids"
        )
        for study in cooled_down_studies:
            new_id = study.study_id + "_" + random_string(8)
            self.logger.debug(f"Pushing {study.study_id}, renaming to {new_id}")
            self.cooled_down.push_study(study, study_id=new_id)

        self.logger.debug("Moving cooled down studies to pending for anonymization")
        self.pending.push_studies(self.cooled_down.get_all_studies())

        self.logger.debug("Empty old trash")
        self.trash.delete_all()

        self.logger.debug(
            f"Moving finished studies older than "
            f"{self.finished.cool_down} minutes to trash"
        )
        self.trash.push_studies(self.finished.get_all_cooled_studies())

    def update_idis_status(self, studies: List[IDISStudy]):
        """Query IDIS to update the status of all given studies"""

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

    def get_pending_studies(self):
        """Find all studies that have been sent to IDIS and are pending
        anonymization. Move studies to errored if things go wrong
        """
        try:
            studies = self.pending.get_all_studies()
        except RecordNotFoundException as e:
            self.logger.warning(f"A record is missing. Original exception: {e}")
            orphaned = self.pending.get_all_orphaned_studies()
            self.logger.warning(
                f" Moving {len(orphaned)} orphaned studies to errored and trying "
                f"again. Studies moved: {[x.study_id for x in orphaned]}"
            )
            self.errored.push_studies(orphaned)
            studies = self.pending.get_all_studies()
        return studies


class ObjectNotFound(IDISSendException):
    pass
