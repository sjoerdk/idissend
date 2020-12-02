"""Classes and methods for controlling a pipeline instance"""

from typing import List

from idissend.core import Study
from idissend.orm import IDISRecord
from idissend.pipeline import IDISPipeline, Pipeline


class Admin:
    """Control a pipeline instance.

    Separate object from pipeline class due to separate goals:

    * Pipeline instances should have methods that favor completeness
    * Admin instances do not have to be complete, but should be quick and useful

    In addition, admin uses strings to refer to objects and handles missing objects.
    Pipeline instances work with python objects directly

    """

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def status(self) -> str:
        """Concise overview of this pipeline"""
        return self.pipeline.get_status()

    def list_studies(self, stage: str, ids_only: bool = False) -> str:
        """List all studies for given stage

        Parameters
        ----------
        stage: str
            name of the stage to list
        ids_only: bool, optional
            If True, print only study ids. If False, print study description.
            Defaults to False

        Returns
        -------
        newline separated information for each study in stage

        """
        stage = self.pipeline.get_stage(stage)
        if ids_only:
            return "\n".join([x.study_id for x in stage.get_all_studies()])
        else:
            return "\n".join([str(x) for x in stage.get_all_studies()])

    def move_studies(self, ids: List[str], to_stage: str) -> str:
        """List all studies for given stage

        Parameters
        ----------
        ids: List[str]
            Move studies with these ids
        to_stage: str
            To this stage

        Returns
        -------
        newline separated information for each study in stage

        """
        stage = self.pipeline.get_stage(to_stage)
        stage.push_studies(self.pipeline.get_studies(study_ids=ids))


class IDISAdmin(Admin):
    """Admin for IDISPipeline instances. Additional functions for interacting
    with IDIS
    """

    def __init__(self, pipeline: IDISPipeline):
        super().__init__(pipeline)
        self.pipeline = pipeline

    def get_idis_records(self, study_ids: List[str]) -> List[IDISRecord]:
        """Find the stored idis job info for each given study

        Notes
        -----
        Skips study ids for which no record can be found. Output list might
        be smaller than input list

        Parameters
        ----------
        study_ids: List[str]
            study ids to look up

        Returns
        -------
        List[IDISRecord]
            Record for each study id found. If not found, study id will be skipped

        """

        return self.pipeline.get_idis_records(
            self.pipeline.get_studies(study_ids=study_ids)
        )

    def process_with_idis(
        self, study_ids: List[str], create_new_job=False
    ) -> List[Study]:
        """Move each study to pending stage and reset the existing idis job

        Parameters
        ----------
        study_ids: List[str]
            ids of idissend studies to process with idis. Will reset IDIS job
            if one exists
        create_new_job: Bool, optional
            if True, set existing IDIS job to inactive, delete record and create
            a new IDIS job. Defaults to False

        Returns
        -------
        List[Study]
        """
        # TODO: continue, implement reset in pending stage
        # get studies
        #
        # push to pending which will reset automatically
        # if create new job, first delete existing records and then push
        pass

    def get_job_ids(self, study_ids: List[str]) -> List[str]:
        """Find the idis job id for each given study

        Parameters
        ----------
        study_ids: List[str]
            study ids to look up

        Returns
        -------
        List[str]
            For each study for which a record was found, an IDIS job id

        Notes
        -----
        Output might be smaller than input. Missing records are skipped.

        """
        return [str(x.job_id) for x in self.get_idis_records(study_ids)]

    def get_error_messages(self, study_ids: List[str]) -> List[str]:
        """Find the idis job id for each given study

        Parameters
        ----------
        study_ids: List[str]
            study ids to look up

        Returns
        -------
        List[str]
            For each study for which a record was found, an IDIS job id

        Notes
        -----
        Output might be smaller than input. Missing records are skipped.

        """
        return [
            x.study_id + " : " + str(x.last_error_message)
            for x in self.get_idis_records(study_ids)
        ]
