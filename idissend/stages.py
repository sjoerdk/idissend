"""Specific implementations of stages the data goes through"""
import shutil

from anonapi.client import AnonClientTool, ClientToolException
from anonapi.exceptions import AnonAPIException
from anonapi.objects import RemoteAnonServer
from anonapi.paths import UNCMapping, UNCMappingException
from anonapi.responses import JobInfo, JobsInfoList
from collections import defaultdict
from datetime import datetime
from idissend.core import Stage, Stream, Study, PushStudyCallbackException
from idissend.exceptions import IDISSendException
from idissend.orm import IDISRecord
from idissend.persistence import IDISSendRecords
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Iterable, Tuple


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
            Human readable study_id for this stage
        path: str
            Root path of this folder
        streams: List[Stream]
            All the streams for which this folder could receive data
        cool_down: int, optional
            Number of minutes to wait before considering a study cooled down.
            CoolDown is measured against the last modification date of any file
            in the study. Defaults to 5 minutes
        """
        super().__init__(name=name, path=path, streams=streams)
        self.cool_down = cool_down

    def get_all_studies(self) -> List[Study]:
        """Get all studies for all streams in this folder"""
        return super().get_all_studies()

    def get_all_cooled_studies(self) -> List[Study]:
        """Get all studies which have not changed in the cool down period"""
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
        """Find the IDIS server with the given study_id

        Raises
        ------
        UnknownServerException
            When no server can be found with that study_id
        """
        server = {x.name: x for x in self.servers}.get(server_name)
        if server:
            return server
        else:
            raise UnknownServerException(
                f"Server '{server_name}' not found. Known s"
                f"ervers:{[str(x) for x in self.servers]}"
            )


class PendingAnon(Stage):
    """Stage that communicates with IDIS for anonymization. Monitors progress
    of anonymization. This stage is the single entrypoint for all dealings with
    IDIS. Other stages can request IDIS information via this stage.

    * Caches job status in a local db
    * Communicates with IDIS to get info if needed
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
            Human readable study_id for this stage
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

        super().__init__(name=name, path=path, streams=streams)
        self.idis_connection = idis_connection
        self.records = records
        self.unc_mapping = unc_mapping

    def idis_client_tool(self) -> AnonClientTool:
        """Allows you to talk to IDIS"""
        return self.idis_connection.client_tool

    def push_study_callback(self, study: Study) -> Study:
        """Resets existing IDIS job or creates new for the given study

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
        self.assert_active_idis_job(study)

        return study

    def assert_active_idis_job(self, study: Study):
        """Resets existing IDIS job or creates new for the given study

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
        server = self.idis_connection.servers[0]
        with self.records.get_session() as session:
            existing_record = session.get_for_study_id(study.study_id)
        if existing_record:
            # an IDIS job has been created before. Reset.
            job_id = existing_record.job_id
            self.reset_idis_job(server=server, job_id=job_id)
        else:
            # No IDIS job exists. Create a new one
            created = self.create_idis_job(server, study)

            # save job id for this study to check back on later
            try:
                with self.records.get_session() as session:
                    session.add(
                        study_id=study.study_id,
                        job_id=created.job_id,
                        server_name=server.name,
                    )
            except SQLAlchemyError as e:
                raise PushStudyCallbackException(e)

    def reset_idis_job(self, server: RemoteAnonServer, job_id: str):
        """Reset the given IDIS job on the given server

        Raises
        ------
        PushStudyCallbackException
            If anything goes wring communicating with IDIS
        """

        tool = self.idis_client_tool()
        result = tool.reset_job(server=server, job_id=job_id)
        if result.startswith("Error"):
            # anonapi does not raise exceptions here. Working around this.
            # Should be changed (see idissend #290)"""
            raise PushStudyCallbackException(result)
        else:
            self.logger.debug(f"reset job: {result}")

    def create_idis_job(self, server: RemoteAnonServer, study: Study) -> JobInfo:
        """Create a job on IDIS server that will anonymize study

        Raises
        ------
        PushStudyCallbackException
            If anything goes wrong creating this job

        """
        source_path = study.get_path()
        destination_path = study.stream.output_folder

        if self.unc_mapping:
            try:
                source_path, destination_path = (
                    self.unc_mapping.to_unc(x) for x in (source_path, destination_path)
                )
            except UNCMappingException as e:
                raise PushStudyCallbackException(e)

        try:
            job = self.idis_client_tool().create_path_job(
                server=server,
                project_name=study.stream.idis_profile_name,
                source_path=source_path,
                destination_path=destination_path,
                description=f"Created by idissend for " f"stream " f"{study.stream}",
                pims_keyfile_id=study.stream.pims_key,
            )
            created = job
            self.logger.info(
                f"Created IDIS job {created.job_id} on {server} " f"for {study}"
            )
        except AnonAPIException as e:
            raise PushStudyCallbackException(e)
        return created

    def get_records(self, studies: List[Study]) -> List[IDISRecord]:
        """Look up the record for each study in local records db

        Parameters
        ----------
        studies: List[Study]
            Study objects to turn onto IDISStudy

        Raises
        ------
        RecordNotFoundException
            If any study has no record in the records database
        """
        records = []
        with self.records.get_session() as session:
            for study in studies:
                record = session.get_for_study_id(study.study_id)
                if not record:
                    raise RecordNotFoundException(
                        f"{str(self)}: There is no record for {study}", study=study
                    )
                else:
                    records.append(record)
        return records

    def get_all_orphaned_studies(self) -> List[Study]:
        """Returns all studies for which no records exists.

        This should not occur often but could be the result of certain crashes or if
        data has been inserted into streams from outside idissend. This is the
        only method in this stage which does not return IDISStudy objects.

        Returns
        -------
        List[Study]
            All studies for which no record exists.
        """

        with self.records.get_session() as session:
            records = session.get_all()

        studies = super().get_all_studies()
        study_ids = {x.study_id for x in records}
        return [x for x in studies if x.study_id not in study_ids]

    def get_server(self, server_name: str) -> RemoteAnonServer:
        return self.idis_connection.get_server(server_name=server_name)

    def update_records(self, studies: List[Study]) -> List[Tuple[Study, IDISRecord]]:
        """Contact IDIS to get updated status for all given studies

        Raises
        ------
        IDISCommunicationException
            If anything goes wrong getting information from IDIS
        RecordNotFoundException
            If any study has no record in the records database
        """
        records = self.get_records(studies)
        # group jobs per IDIS server to minimize number of web API queries
        records_per_server = defaultdict(list)
        for record in records:
            records_per_server[self.get_server(record.server_name)].append(record)

        # now contact each server in turn to get updated job info
        job_infos = []
        for server in records_per_server:
            # get info from IDIS
            job_infos += self.get_job_info_list(
                server=server, job_ids=[x.job_id for x in records_per_server[server]]
            )

        # We should now have new info for each study. Update local records with this
        record_ids = {x.study_id: x for x in records}
        job_info_ids = {x.job_id: x for x in job_infos}

        with self.records.get_session() as session:
            for study in studies:
                record = record_ids[study.study_id]
                try:
                    job_info = job_info_ids[record.job_id]
                except KeyError:
                    raise IDISCommunicationException(
                        f"{study} is associated with IDIS job {record.job_id}, but "
                        f"IDIS server did not return any info for this job"
                    )

                record.last_status = job_info.status
                record.last_error_message = job_info.error
                record.last_check = datetime.now()
                session.add_record(record)

        return [(study, record_ids[study.study_id]) for study in studies]

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

    def get_all_records(self) -> List[IDISRecord]:
        """Return all records from local db"""
        with self.records.get_session() as session:
            return session.get_all()


class Trash(Stage):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)
    """

    def delete_all(self):
        """Delete data for all studies in trash"""
        studies = self.get_all_studies()
        self.logger.info(
            f"Removing data for {len(studies)} studies: {[str(x) for x in studies]}"
        )
        for study in studies:
            shutil.rmtree(study.get_path())


class RecordNotFoundException(IDISSendException):
    def __init__(self, *args, study: Optional[Study] = None, **kwargs):
        """

        Parameters
        ----------
        study: Optional[Study]
            The study associated with this exception. Defaults to None
        """
        super().__init__(args, kwargs)
        self.study = study

    pass


class IDISCommunicationException(IDISSendException):
    """Something went wrong getting info from IDIS"""

    pass


class UnknownServerException(IDISCommunicationException):
    pass
