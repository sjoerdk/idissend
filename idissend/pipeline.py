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


# parameters #

BASE_PATH = Path("/tmp/idissend")  # all data for all stages goes here
STAGES_BASE_PATH = BASE_PATH / "stages"

DEFAULT_IDIS_PROJECT = "An_idis_project"  # anonymize with this profile
DEFAULT_PIMS_KEY = "123"  # Pass this to IDIS for generating pseudonyms

IDIS_USERNAME = "SVC1234"  # use this to identify with IDIS web API
IDIS_TOKEN = "a_token"

IDIS_WEB_API_SERVER_URL = (
    "https://umcradanonp11.umcn.nl/p01"  # Talk to IDIS through this
)
IDIS_WEB_API_SERVER_NAME = "p01"  # Name to use in log messages

OUTPUT_BASE_PATH = Path(r"\\server\path")  # let IDIS write all data here

# init #
STAGES_BASE_PATH.mkdir(parents=True, exist_ok=True)  # assert base dir exists

# Indicate which local paths correspond to which UNC paths.
# This makes it possible to expose local data to IDIS servers
unc_mapping = UNCMapping([UNCMap(local=Path("/"), unc=UNCPath(r"\\server\share"))])

# streams #
# the different routes data can take through the pipeline. Data will always stay
# inside the same stream
streams = [
    Stream(
        name="stream1",
        output_folder=OUTPUT_BASE_PATH / "stream1",
        idis_project=DEFAULT_IDIS_PROJECT,
        pims_key=DEFAULT_PIMS_KEY,
        contact=Person(name="Sjoerd", email="mock_email"),
    ),
    Stream(
        name="stream2",
        output_folder=OUTPUT_BASE_PATH / "stream2",
        idis_project=DEFAULT_IDIS_PROJECT,
        pims_key=DEFAULT_PIMS_KEY,
        contact=Person(name="Sjoerd2", email="mock_email"),
    ),
]

# stages #
# data in one stream goes through one or more of these stages
incoming = CoolDown(
    name="incoming", path=STAGES_BASE_PATH / "incoming", streams=streams, cool_down=0
)

connection = IDISConnection(
    client_tool=AnonClientTool(username=IDIS_USERNAME, token=IDIS_TOKEN),
    servers=[
        RemoteAnonServer(name=IDIS_WEB_API_SERVER_NAME, url=IDIS_WEB_API_SERVER_URL)
    ],
)

records = IDISSendRecords(
    session_maker=get_db_sessionmaker(STAGES_BASE_PATH / "records_db.sqlite")
)

pending = PendingAnon(
    name="pending",
    path=STAGES_BASE_PATH / "pending",
    streams=streams,
    idis_connection=connection,
    records=records,
    unc_mapping=unc_mapping,
)

errored = Stage(name="errored", path=STAGES_BASE_PATH / "errored", streams=streams)

finished = CoolDown(
    name="finished",
    path=STAGES_BASE_PATH / "finished",
    streams=streams,
    cool_down=2 * 60 * 24,
)  # 2 days

trash = Trash(name="trash", path=STAGES_BASE_PATH / "trash", streams=streams)


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
        cooled_down = self.incoming.get_all_studies(only_cooled=True)
        self.logger.debug(f"Found {len(cooled_down)}. Pushing to pending")
        self.pending.push_studies(cooled_down)

        self.logger.debug(
            f"Moving finished studies older than "
            f"{self.finished.cool_down} minutes to trash"
        )
        self.trash.push_studies(self.finished.get_all_studies())

        self.logger.debug("empty trash if needed")
        self.trash.empty()

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
