"""Specific implementations of stages the data goes through"""
import shutil

from anonapi.client import AnonClientTool, ClientToolException
from anonapi.exceptions import AnonAPIException
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMapping, UNCMappingException
from anonapi.responses import JobsInfoList
from collections import defaultdict
from datetime import datetime
from idissend.core import Stage, Stream, Study, PushStudyCallbackException
from idissend.exceptions import IDISSendException
from idissend.orm import PendingAnonRecord
from idissend.persistence import IDISSendRecords
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Iterable


class CoolDown(Stage):
    """A stage with an inbuilt waiting or cool down period.

    By default studies are only returned after they have been in the stage for this
    period. There are two obvious use cases for this:

    * Making sure a DICOM transfer is complete before starting processing. DICOM
      CSTORE infamously does not have a concept of 'all files have been sent'.
      In the dicom world any file could come in at any time. A common hack to
      get around this is to assume that if no files have been added for x minutes
      the transfer is probably complete.
    * Removing completed and errored studies after a certain grace period.

    """

    def __init__(
        self, name: str, path: Path, streams: List[Stream], cool_down: int = 5
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
        cool_down: int, optional
            Number of minutes to wait before considering a study cooled down.
            CoolDown is measured against the last modification date of any file
            in the study. Defaults to 5 minutes
        """
        super(CoolDown, self).__init__(name=name, path=path, streams=streams)
        self.cool_down = cool_down

    def get_all_studies(self) -> List[Study]:
        """Get all studies for all streams in this folder

        """
        return super(CoolDown, self).get_all_studies()

    def get_all_cooled_studies(self) -> List[Study]:
        """Get all studies which have not changed in the cool down period

        """
        return [x for x in self.get_all_studies() if self.has_cooled_down(x)]

    def has_cooled_down(self, study: Study) -> bool:
        """Check whether files are still coming in for this study

        Considered cooled down if no file was modified less then <cool_down> mins ago
        """
        return study.is_older_than(self.cool_down)


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
                f"ervers:{[str(x) for x in self.servers]}"
            )


class PendingStudy(Study):
    """A study pending anonymization. Has additional links to check its status"""

    def __init__(
        self, name: str, stream: Stream, stage: Stage, record: PendingAnonRecord
    ):
        super(PendingStudy, self).__init__(name, stream, stage)
        self.record = record

    @property
    def last_status(self) -> str:
        """Get last known status of IDIS job for this study from records

        Returns
        -------
        str
            One of anonapi.responses.JobStatus
        """
        return self.record.last_status

    @property
    def last_check(self) -> Optional[datetime]:
        """Date when the job for this study was last updated"""
        return self.record.last_check

    @property
    def job_id(self) -> int:
        """IDIS job id for the job associated with this study"""
        return self.record.job_id

    @property
    def server_name(self) -> str:
        return self.record.server_name


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
        unc_mapping: UNCMapping = None,
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
        unc_mapping: UNCMapping, optional
            Translates any local paths to their UNC equivalents, making sure
            only UNC paths get sent to IDIS. If not given, use any paths as-is
        """

        super(PendingAnon, self).__init__(name=name, path=path, streams=streams)
        self.idis_connection = idis_connection
        self.records = records
        self.unc_mapping = unc_mapping

    def push_study_callback(self, study: Study) -> PendingStudy:
        """Creates an IDIS job for the given study

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

        # Create a job
        client = self.idis_connection.client_tool
        server = self.idis_connection.servers[0]
        source_path = study.get_path()
        destination_path = study.stream.output_folder
        if self.unc_mapping:
            try:
                source_path = self.unc_mapping.to_unc(source_path)
                destination_path = self.unc_mapping.to_unc(destination_path)
            except UNCMappingException as e:
                raise PushStudyCallbackException(e)

        try:
            created = client.create_path_job(
                server=server,
                project_name=study.stream.idis_project,
                source_path=source_path,
                destination_path=destination_path,
                description=f"Created by idissend for stream " f"{study.stream}",
                pims_keyfile_id=study.stream.pims_key,
            )
            self.logger.info(
                f"Created IDIS job {created.job_id} on {server} " f"for {study}"
            )
        except AnonAPIException as e:
            raise PushStudyCallbackException(e)

        # save job id for this study to check back on later
        try:
            with self.records.get_session() as session:
                record = session.add(
                    study_folder=study.get_path(),
                    job_id=created.job_id,
                    server_name=server.name,
                )

        except SQLAlchemyError as e:
            raise PushStudyCallbackException(e)

        return self.to_pending_study(study=study, record=record)

    def get_all_studies(self) -> List[PendingStudy]:
        """PendingAnon returns PendingStudy objects, which hold additional info on
        IDIS job status """
        studies = super().get_all_studies()
        return [self.to_pending_study(x) for x in studies]

    def get_studies(self, stream: Stream) -> List[PendingStudy]:
        """Get all studies for the given stream

        """
        return [self.to_pending_study(x) for x in super().get_studies(stream=stream)]

    def to_pending_study(
        self, study: Study, record: PendingAnonRecord = None
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
            with self.records.get_session() as session:
                record = session.get_for_study_folder(study.get_path())
        if not record:
            raise RecordNotFoundException(
                f"{str(self)}: There is no record for {study}"
            )
        return PendingStudy(
            name=study.name, stream=study.stream, stage=study.stage, record=record
        )

    def get_all_orphaned_studies(self) -> List[Study]:
        """Returns all studies for which no records exists.

        This should not occur often but could be the result of certain crashes or if
        data has been inserted into streams from outside idissend. This is the
        only method in this stage which does not return PendingStudy objects.

        Returns
        -------
        List[Study]
            All studies for which no record exists.
        """

        with self.records.get_session() as session:
            records = session.get_all()

        studies = super(PendingAnon, self).get_all_studies()
        study_paths = {x.get_path(): x for x in studies}
        record_paths = set((x.study_folder for x in records))
        return [y for x, y in study_paths.items() if x not in record_paths]

    def get_server(self, server_name: str) -> RemoteAnonServer:
        return self.idis_connection.get_server(server_name=server_name)

    def update_records(self, studies: List[PendingStudy]) -> List[PendingStudy]:
        """Contact IDIS to get updated status for all given studies

        Raises
        ------
        IDISCommunicationException
            If anything goes wrong getting information from IDIS
        """
        # group jobs per IDIS server to minimize number of web API queries
        studies_per_server = defaultdict(list)
        for study in studies:
            studies_per_server[self.get_server(study.server_name)].append(study)

        job_info_per_study = {}
        for server in studies_per_server:
            # get info from IDIS
            job_infos = self.get_job_info_list(
                server=server, job_ids=[x.job_id for x in studies_per_server[server]]
            )

            # associate what you get back with studies
            for study in studies_per_server[server]:
                # job infos might nog be in the same order, find them again
                info = {x.job_id: x for x in job_infos}.get(study.job_id)
                if not info:
                    raise IDISCommunicationException(
                        f"study '{study}' should have a job with id {study.job_id} "
                        f"in {server}, but that job id does not seem to exist there"
                    )
                job_info_per_study[study] = info

        # now update all records with the gathered IDIS data
        with self.records.get_session() as session:
            for study, job_info in job_info_per_study.items():
                study.record.last_status = job_info.status
                study.record.last_check = datetime.now()
                session.add_record(study.record)

        return list(job_info_per_study.keys())

    def get_job_info_list(
        self, server: RemoteAnonServer, job_ids: Iterable[int]
    ) -> JobsInfoList:
        """Contact IDIS server for updated info given jobs

        Raises
        ------
        IDISCommunicationException
            If anything goes wrong communicating with IDIS

        """
        try:
            return self.idis_connection.client_tool.get_job_info_list(
                server=server, job_ids=list(job_ids)
            )
        except ClientToolException as e:
            raise IDISCommunicationException(e)


class Trash(Stage):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)"""

    def delete_all(self):
        """delete data for all studies in trash"""
        studies = self.get_all_studies()
        self.logger.info(
            f"Removing data for {len(studies)} studies: " f"{[str(x) for x in studies]}"
        )
        for study in studies:
            shutil.rmtree(study.get_path())


class RecordNotFoundException(IDISSendException):
    pass


class IDISCommunicationException(IDISSendException):
    """Something went wrong getting info from IDIS"""

    pass


class UnknownServerException(IDISCommunicationException):
    pass
