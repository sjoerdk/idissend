"""core and stages define the pieces of a pipeline,
this module has classes and methods for the relationships between them
"""


import logging
from pathlib import Path
from typing import List

from anonapi.client import AnonClientTool
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMap, UNCMapping, UNCPath

from anonapi.responses import JobStatus
from collections import Counter
from idissend.core import Person, Stage, Stream, random_string
from idissend.persistence import IDISSendRecords, get_db_sessionmaker
from idissend.stages import (
    CoolDown,
    IDISConnection,
    PendingAnon,
    PendingStudy,
    RecordNotFoundException,
    Trash,
)


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
        self.all_stages = [incoming, cooled_down, pending, finished, trash, errored]
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

    def update_idis_status(self, studies: List[PendingStudy]):
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

    def assert_all_paths(self):
        """Make sure folders for each stage and stream in this pipeline exist"""
        for stage in self.all_stages:
            stage.assert_all_paths()


def get_pipeline_instance(base_path: str = "/tmp/idissend") -> DefaultPipeline:
    """Generate a pipeline instance based at the given path

    Parameters
    ----------
    base_path: str
        root path for all data in this pipeline

    Returns
    -------
    DefaultPipeline
        A default pipeline instance with a lot of default settings

    """
    # parameters #
    base_path = Path(base_path)  # all data for all stages goes here
    stages_base_path = base_path / "stages"

    default_idis_profile_name = "An_idis_profile"  # anonymize with this profile
    default_pims_key = "123"  # Pass this to IDIS for generating pseudonyms

    idis_username = "SVC1234"  # use this to identify with IDIS web API
    idis_token = "a_token"

    idis_web_api_server_url = (
        "https://umcradanonp11.umcn.nl/t01"  # Talk to IDIS through this
    )
    idis_web_api_server_name = "t01"  # Name to use in log messages

    records_db_url = f"sqlite:///{stages_base_path / 'records_db.sqlite'}"

    output_base_path = Path(r"\\server\path")  # let IDIS write all data here

    # init #
    stages_base_path.mkdir(parents=True, exist_ok=True)  # assert base dir exists

    # Indicate which local paths correspond to which UNC paths.
    # This makes it possible to expose local data to IDIS servers
    unc_mapping = UNCMapping([UNCMap(local=Path("/"), unc=UNCPath(r"\\server\share"))])

    # streams #
    # the different routes data can take through the pipeline. Data will always stay
    # inside the same stream
    streams = [
        Stream(
            name="stream1",
            output_folder=output_base_path / "stream1",
            idis_profile_name=default_idis_profile_name,
            pims_key=default_pims_key,
            contact=Person(name="Sjoerd", email="mock_email"),
        ),
        Stream(
            name="stream2",
            output_folder=output_base_path / "stream2",
            idis_profile_name=default_idis_profile_name,
            pims_key=default_pims_key,
            contact=Person(name="Sjoerd2", email="mock_email"),
        ),
    ]

    # stages #
    # data in one stream goes through one or more of these stages
    incoming = CoolDown(
        name="incoming",
        path=stages_base_path / "incoming",
        streams=streams,
        cool_down=0,
    )

    cooled_down = Stage(
        name="cooled_down", path=stages_base_path / "cooled_down", streams=streams
    )

    connection = IDISConnection(
        client_tool=AnonClientTool(username=idis_username, token=idis_token),
        servers=[
            RemoteAnonServer(name=idis_web_api_server_name, url=idis_web_api_server_url)
        ],
    )

    records = IDISSendRecords(session_maker=get_db_sessionmaker(records_db_url))

    pending = PendingAnon(
        name="pending",
        path=stages_base_path / "pending",
        streams=streams,
        idis_connection=connection,
        records=records,
        unc_mapping=unc_mapping,
    )

    errored = Stage(name="errored", path=stages_base_path / "errored", streams=streams)

    finished = CoolDown(
        name="finished",
        path=stages_base_path / "finished",
        streams=streams,
        cool_down=2 * 60 * 24,
    )  # 2 days

    trash = Trash(name="trash", path=stages_base_path / "trash", streams=streams)

    return DefaultPipeline(
        incoming=incoming,
        cooled_down=cooled_down,
        pending=pending,
        finished=finished,
        trash=trash,
        errored=errored,
    )
