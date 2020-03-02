"""Specific implementations of stages the data goes through"""

from pathlib import Path
from typing import List

from anonapi.client import AnonClientTool
from anonapi.exceptions import AnonAPIException
from anonapi.objects import RemoteAnonServer
from sqlalchemy.exc import SQLAlchemyError

from idissend.core import Stage, Stream, Study, PushStudyCallbackException
from idissend.exceptions import IDISSendException
from idissend.orm import PendingAnonRecord
from idissend.persistence import IDISSendRecords


class Incoming(Stage):
    """A folder where DICOM files are coming into multiple streams
    Assumed directory structure is <stream>/<study>/file

    An Incoming can come up with studies that are deemed complete (see init
    notes below for cooldown)
    """

    def __init__(self, name: str, path: Path, streams: List[Stream], cooldown: int = 5):
        """

        Parameters
        ----------
        name: str
            Human readable name for this stage
        path: str
            Root path of this folder
        streams: List[Stream]
            All the streams for which this folder could receive data
        cooldown: int
            Number of minutes to wait before considering a study 'complete'. This
            is needed because a DICOM cstore transfer has no concept of a transfer
            being complete; files just come in and they might stop coming and or
            they might not. Who knows.
            The assumption that this class makes is 'if files have stopped coming
            in for <cooldown> minutes, the transfer is probably done'. Bit sad
            but the only way to do this with DICOM cstore.
        """
        super(Incoming, self).__init__(name=name, path=path, streams=streams)
        self.cooldown = cooldown

    def get_all_studies(self, only_cooled=True) -> List[Study]:
        """Get all studies for all streams in this folder

        Parameters
        ----------
        only_cooled: bool, optional
            If True, return only studies deemed complete after cooldown.
            Otherwise, return all studies. Defaults to True

        """
        studies = super(Incoming, self).get_all_studies()
        if only_cooled:
            studies = [x for x in studies if self.has_cooled_down(x)]

        return studies

    def get_stream_folder(self, stream: Stream) -> Path:
        """Get the folder where data is coming in for this stream """
        return self.get_path_for_stream(stream)

    def has_cooled_down(self, study: Study) -> bool:
        """Check whether files are still coming in for this study

        Considered cooled down if no file was modified less then <cooldown> mins ago
        """
        return study.is_older_than(self.cooldown)


class IDISConnection:
    """Everything you need to talk to the IDIS anonymization server"""

    def __init__(self, client_tool: AnonClientTool, servers: List[RemoteAnonServer]):
        self.client_tool = client_tool
        self.servers = servers

    def __str__(self):
        return f"Connection with IDIS servers {[str(x) for x in self.servers]}"


class PendingStudy(Study):
    """A study pending anonymization. Has additional links to check its status"""

    def __init__(
        self, name: str, stream: Stream, stage: "Stage", record: PendingAnonRecord
    ):
        super(PendingStudy, self).__init__(name, stream, stage)
        self.record = record

    def status(self) -> str:
        """Get last known status of IDIS job for this study from records

        Returns
        -------
        str
            One of anonapi.responses.JobStatus
        """
        pass

    def update_status(self):
        """Query IDIS to update the status of this study"""
        pass


class PendingAnon(Stage):
    """Stage where data is presented to IDIS for anonymization. Monitors progress
    of anonymization and removes data when anonymization is done or failed.

    Communicates with IDIS to get info
    """

    def __init__(
        self,
        name: str,
        path: Path,
        streams: List[Stream],
        idis_connection: IDISConnection,
        records: IDISSendRecords,
    ):
        """

        Parameters
        ----------
        name: str
            Human readable name for this stage
        path: str
            Root path of this folder
        streams: List[Stream]
            All the streams for which this folder could receive data
        idis_connection: IDISConnection
            Used for communicating with IDIS
        records: IDISSendRecords
            Persistent storage for which studies have been sent to IDIS
        """

        super(PendingAnon, self).__init__(name=name, path=path, streams=streams)
        self.idis_connection = idis_connection
        self.records = records

    def push_study_callback(self, study: Study) -> Study:
        """
        Parameters
        ----------
        study: Study
            Study that was just pushed

        Raises
        ------
        PushStudyCallbackException
            When anything goes wrong executing this callback

        Returns
        -------
        Study
            The study after executing the callback

        """

        # * Create a job
        client = self.idis_connection.client_tool
        try:
            created = client.create_path_job(
                server=self.idis_connection.servers[0],
                project_name=study.stream.idis_project,
                source_path=study.path,
                destination_path=study.stream.output_folder,
                description=f"Created by idissend for stream " f"{study.stream}",
                pims_keyfile_id=study.stream.idis_project,
            )
        except AnonAPIException as e:
            raise PushStudyCallbackException(e)

        # save job id for this study to check back on later
        try:
            self.records.add(
                study_folder=study.path,
                job_id=created.job_id,
                server_name=study.stream.idis_project,
            )
        except SQLAlchemyError as e:
            raise PushStudyCallbackException(e)

        return study

    def get_pending_studies(self) -> List[PendingStudy]:
        """All studies in this stage as PendingStudy objects"""

        return [self.to_pending_study(x) for x in self.get_all_studies()]

    def to_pending_study(self, study: Study) -> PendingStudy:
        """Find job information for this record and append

        Raises
        ------
        RecordNotFoundException
            If the given study has no record in the records database
        """
        record = self.records.get_for_study_folder(study.path)
        if not record:
            raise RecordNotFoundException(f"There is no record for  {study}")
        return PendingStudy(
            name=study.name, stream=study.stream, stage=study.stage, record=record
        )


class Trash(Stage):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)"""

    pass


class RecordNotFoundException(IDISSendException):
    pass
