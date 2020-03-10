"""An example of a pipeline receiving data and sending it to IDIS.

streams and users are all hardcoded in this minimal example, but will probably
be in a database for production
"""

import logging
from pathlib import Path

from anonapi.client import AnonClientTool
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMapping, UNCMap, UNCPath

from idissend.core import Stream, Person, Stage
from idissend.persistence import IDISSendRecords, get_db_sessionmaker
from idissend.pipeline import DefaultPipeline
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

RECORDS_DB_PATH = STAGES_BASE_PATH / 'records_db.sqlite'

OUTPUT_BASE_PATH = Path(r'\\server\path')  # let IDIS write all data here

# init #
STAGES_BASE_PATH.mkdir(parents=True, exist_ok=True)  # assert base dir exists

# Indicate which local paths correspond to which UNC paths.
# This makes it possible to expose local data to IDIS servers
unc_mapping = UNCMapping(
    [UNCMap(local=Path('/'), unc=UNCPath(r'\\server\share'))])

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
    RECORDS_DB_PATH))

pending = PendingAnon(name='pending',
                      path=STAGES_BASE_PATH / 'pending',
                      streams=streams,
                      idis_connection=connection,
                      records=records,
                      unc_mapping=unc_mapping
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

pipeline = DefaultPipeline(incoming=incoming,
                           pending=pending,
                           finished=finished,
                           trash=trash,
                           errored=errored)


logging.basicConfig(format="%(asctime)s %(name)-40s %(levelname)-8s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.info("Running once")

pipeline.incoming.incoming.assert_all_paths()
pipeline.run_once()
