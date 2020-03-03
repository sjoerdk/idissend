"""An example of a pipeline receiving data and sending it to IDIS.

streams and users are all hardcoded in this minimal example, but will probably
be in a database for production
"""

import logging
from pathlib import Path
from typing import List

from anonapi.client import AnonClientTool
from anonapi.objects import RemoteAnonServer
from anonapi.responses import JobStatus

from idissend.core import Stream, Person, Stage
from idissend.persistence import IDISSendRecords, get_db_sessionmaker
from idissend.stages import Incoming, PendingAnon, IDISConnection, Trash

# parameters #

BASE_PATH = Path('/tmp/idissend')  # all data for all stages goes here
STAGES_BASE_PATH = BASE_PATH / 'stages'

DEFAULT_IDIS_PROJECT = 'An_idis_project'  # anonymize with this profile
DEFAULT_PIMS_KEY = '123'  # Pass this to IDIS for generating pseudonyms

IDIS_USERNAME = 'SVC1234'  # use this to identify with IDIS web API
IDIS_TOKEN = 'a_token'

IDIS_WEB_API_SERVER_URL = 'https://umcradanonp11.umcn.nl/p01'  # Talk to IDIS through this
IDIS_WEB_API_SERVER_NAME = 'p01'  # Name to use in log messages

OUTPUT_BASE_PATH = Path(r'\\server\path')

# init #
STAGES_BASE_PATH.mkdir(parents=True, exist_ok=True)  # assert base dir exists

# streams #
# the different routes data can take through the pipeline. Data will always stay
# inside the same stream
streams = [Stream(name='stream1',
                  output_folder=OUTPUT_BASE_PATH / 'stream1',
                  idis_project=DEFAULT_IDIS_PROJECT,
                  pims_key=DEFAULT_PIMS_KEY,
                  contact=Person(name='Sjoerd',
                                 email='mock_email')
                  ),
           Stream(name='stream2',
                  output_folder=OUTPUT_BASE_PATH / 'stream2',
                  idis_project=DEFAULT_IDIS_PROJECT,
                  pims_key=DEFAULT_PIMS_KEY,
                  contact=Person(name='Sjoerd2',
                                 email='mock_email')
                  )
           ]

# stages #
# data in one stream goes through one or more of these stages
incoming = Incoming(name='incoming',
                    path=STAGES_BASE_PATH / 'incoming',
                    streams=streams,
                    cooldown=0)

connection = IDISConnection(client_tool=AnonClientTool(username=IDIS_USERNAME,
                                                       token=IDIS_TOKEN),
                            servers=[
                                RemoteAnonServer(name=IDIS_WEB_API_SERVER_NAME,
                                                 url=IDIS_WEB_API_SERVER_URL)])

records = IDISSendRecords(session_maker=get_db_sessionmaker(
    STAGES_BASE_PATH / 'records_db.sqlite'))

pending = PendingAnon(name='pending',
                      path=STAGES_BASE_PATH / 'pending',
                      streams=streams,
                      idis_connection=connection,
                      records=records
                      )

errored = Stage(name='errored',
                path=STAGES_BASE_PATH / 'errored',
                streams=streams)

finished = Stage(name='finished',
                 path=STAGES_BASE_PATH / 'finished',
                 streams=streams)

trash = Trash(name='trash',
              path=STAGES_BASE_PATH / 'trash',
              streams=streams)


# pipeline logic #
# Checks state of data, moves data between stages, empties trash
def run_pipeline_once():
    """Check each stage and perform actions one time"""

    # update the IDIS job status of studies in pending stage
    studies = pending.get_all_studies()
    pending.update_records(studies)

    # deal with different groups of studies
    finished.push_studies([x for x in studies if x.last_status == JobStatus.DONE])
    trash.push_studies([x for x in studies if x.last_status == JobStatus.INACTIVE])
    errored.push_studies([x for x in studies if x.last_status == JobStatus.ERROR])

    # check for new studies coming in
    cooled_down = incoming.get_all_studies(only_cooled=True)
    pending.push_studies(cooled_down)

    # empty trash if needed (disable for now)
    trash.empty()


def print_status(stages: List[Stage]):
    for stage in stages:
        studies = stage.get_all_studies()
        print(f"{stage} at {stage.path} contains {len(studies)} "
              f"studies: {[str(x) for x in studies]}")


logging.basicConfig(format="%(asctime)s %(name)-40s %(levelname)-8s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.info("Running once")


incoming.assert_all_paths()
print_status([incoming, pending, errored, trash])
run_pipeline_once()
