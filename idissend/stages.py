"""Specific implementations of stages the data goes through"""
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from anonapi.client import AnonClientTool, ClientToolException
from anonapi.exceptions import AnonAPIException
from anonapi.objects import RemoteAnonServer
from anonapi.responses import JobInfo, JobsInfoList
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

    def get_server(self, server_name) -> RemoteAnonServer:
        """Find the IDIS server with the given name

        Raises
        ------
        UnknownServerException
            When no server can be found with that name
        """
        server = {x.name: x for x in self.servers}.get(server_name)
        if server:
            return server
        else:
            raise UnknownServerException(
                f"Server '{server_name}' not found. Known s"
                f"ervers:{[str(x) for x in self.servers]}")


class PendingStudy(Study):
    """A study pending anonymization. Has additional links to check its status"""

    def __init__(
        self, name: str, stream: Stream, stage: Stage, record: PendingAnonRecord
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
        return self.record.last_status

    def last_check(self) -> Optional[datetime]:
        """Date when the job for this study was last updated"""
        return self.record.last_check


class PendingAnon(Stage):
    """Stage where data is presented to IDIS for anonymization. Monitors progress
    of anonymization and removes data when anonymization is done or failed.

    * Caches job status in a local db
    * Communicates with IDIS to get info if needed
    * Returns PendingStudy objects instead of Study
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

    def push_study_callback(self, study: Study) -> PendingStudy:
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
        PendingStudy
            The study after executing the callback

        """

        # * Create a job
        client = self.idis_connection.client_tool
        server = self.idis_connection.servers[0]
        try:
            created = client.create_path_job(
                server=server,
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
            with self.records.get_session() as session:
                record = session.add(
                    study_folder=study.path,
                    job_id=created.job_id,
                    server_name=server.name
                )
        except SQLAlchemyError as e:
            raise PushStudyCallbackException(e)

        return self.to_pending_study(study=study, record=record)

    def get_all_studies(self) -> List[PendingStudy]:
        """PendingAnon returns PendingStudy objects, which hold additional info on
        IDIS job status """
        studies = super().get_all_studies()
        return [self.to_pending_study(x) for x in
                studies]

    def get_studies(self, stream: Stream) -> List[PendingStudy]:
        """Get all studies for the given stream

        """
        return [self.to_pending_study(x) for x in
                super().get_studies(stream=stream)]

    def to_pending_study(self, study: Study, record: PendingAnonRecord = None
                         ) -> PendingStudy:
        """Combine Study with a record, turning into PendingStudy. If record
        is not given, look it up in records db

        Parameters
        ----------
        study: Study
            Study object to turn onto PendingStudy
        record: PendingStudy, optional
            The record to include with this study. If not given, look for
            record based on study path

        Raises
        ------
        RecordNotFoundException
            If the given study has no record in the records database
        """
        if not record:
            record = self.records.get_for_study_folder(study.path)
        if not record:
            raise RecordNotFoundException(f"There is no record for {study}")
        return PendingStudy(
            name=study.name, stream=study.stream, stage=study.stage, record=record
        )

    def get_server(self, server_name: str) -> RemoteAnonServer:
        return self.idis_connection.get_server(server_name=server_name)

    def update_records(self, studies: List[PendingStudy]) -> List[PendingStudy]:
        """Contact IDIS to get updated status for all given studies"""
        # group per server and do one query per server
        servers = defaultdict(list)
        for study in studies:
            servers[self.get_server(study.record.server_name)].append(study)


        for server in servers:
            # get ids
            studies = servers[server]
            job_ids = [x.record.job_id for x in studies]
            job_infos = self.get_job_info_list(server=server, job_ids=job_ids)

            # insert status back into studies
            {x.job_id: job_ids for x in job_infos}


    def get_job_info_list(self,
                          server: RemoteAnonServer,
                          job_ids: List[int]) -> JobsInfoList:
        """Contact IDIS server for updated info given jobs

        Raises
        ------
        IDISCommunicationException
            If anything goes wrong communicating with IDIS

        """
        try:
            return self.idis_connection.client_tool.get_job_info_list(
                server=server, job_ids=job_ids)
        except ClientToolException as e:
            IDISCommunicationException(e)


class Trash(Stage):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)"""

    pass


class RecordNotFoundException(IDISSendException):
    pass

class IDISCommunicationException(IDISSendException):
    """Something went wrong getting info from IDIS"""
    pass

class UnknownServerException(IDISCommunicationException):
    pass
