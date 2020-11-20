"""An example of a pipeline receiving data and sending it to IDIS.

streams and users are all hardcoded in this minimal example, but will probably
be in a database for production
"""

import logging

# Make sure you see some logging output
from pathlib import Path

from anonapi.client import AnonClientTool
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMap, UNCMapping, UNCPath

from idissend.core import Person, Stage, Stream
from idissend.persistence import IDISSendRecords, get_db_sessionmaker
from idissend.pipeline import IDISPipeline
from idissend.stages import CoolDown, IDISConnection, PendingAnon, Trash

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_pipeline_instance(base_path: str = "/tmp/idissend") -> IDISPipeline:
    """Generate a pipeline instance at the given path

    Parameters
    ----------
    base_path: str
        root path for all data in this pipeline

    Returns
    -------
    IDISPipeline
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

    return IDISPipeline(
        incoming=incoming,
        cooled_down=cooled_down,
        pending=pending,
        finished=finished,
        trash=trash,
        errored=errored,
    )


pipeline = get_pipeline_instance(base_path="/tmp/idissend")
pipeline.assert_all_paths()
pipeline.run_once()
