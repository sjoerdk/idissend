# -*- coding: utf-8 -*-
import shutil
from datetime import datetime
from pathlib import Path
from typing import List

from anonapi.client import AnonClientTool
from anonapi.exceptions import AnonAPIException
from anonapi.objects import RemoteAnonServer

from idissend.exceptions import IDISSendException


class Person:
    """A person with contact details"""
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email

    def __str__(self):
        return self.name


class Stream:
    """A single route that incoming data goes through.
    Determines anonymization type and destination.

    Notes
    -----
    Responsibilities: A stream should not know about where the data it contains
    is exactly. This is the responsibility of each Stage

    """
    def __init__(self, name: str,
                 output_folder: Path,
                 idis_project: str,
                 pims_key: str,
                 contact: Person):
        """

        Parameters
        ----------
        name: str
            Name of this stream, doubles as folder name
        output_folder: Path
            Write anonymized data to this folder
        idis_project:
            Use the settings in this project for anonymization
        pims_key:
            Use this PIMS project for generating pseudonyms
        contact:
            Who is responsible for collecting this data in the end?
        """

        self.name = name
        self.output_folder = output_folder
        self.idis_project = idis_project
        self.pims_key = pims_key
        self.contact = contact

    def __str__(self):
        return self.name


class AgedPath:
    """A Path with an age() method which yields time since last modification

    makes code cleaner later on
    """

    def __init__(self, path: Path):
        self.path = path

    def __str__(self):
        return str(self.path)

    def age(self) -> float:
        """Minutes since last modification of this file

        Raises
        ------

        """
        delta = datetime.now() - datetime.fromtimestamp(self.path.stat().st_mtime)
        return delta.total_seconds() / 60


class Study:
    """A folder containing files that all belong to the same study """

    def __init__(self, name: str, stream: Stream, stage: 'Stage'):
        self.name = name
        self.stream = stream
        self.stage = stage

    def __str__(self):
        return f'{self.stream}:{self.name}'

    @property
    def path(self) -> Path:
        """Full path to the folder that data for this study is in"""
        return self.stage.get_path_for_study(self)

    def get_files(self) -> List[AgedPath]:
        """All files directly in this folder (no recursing)"""
        return [AgedPath(x) for x in self.path.glob('*') if x.is_file()]

    def age(self) -> float:
        """Minutes since last modification of any file in this study"""
        return min(x.age() for x in self.get_files())

    def is_older_than(self, minutes: float):
        """Is age of each file in this study greater than minutes?

        Potentially faster than age() as it does not check all files
        """
        for file in self.get_files():
            if file.age() <= minutes:
                return False

        return True


class Stage:
    """A distinct step in the pipeline. Contains Studies for different Streams

    A stage is a something like incoming, pending, trash.
    In addition to containing data, a stage might have additional responsibilities
    like communicating with IDIS to determine the status of studies or checking
    file attributes.

    Notes
    -----
    Responsibilities: A stage should know where data is exactly based on
    stream and study


    """
    def __init__(self, name: str, path: Path, streams: List[Stream]):
        """

        Parameters
        ----------
        name: str
            Human readable name for this stage
        path: str
            Root path of this folder
        streams: List[Stream]
            All the streams for which this folder could receive data

        """
        self.name = name
        self.path = path
        self.streams = []
        for stream in streams:
            self.add_stream(stream)

    def __str__(self):
        return self.name

    def push_study(self, study: Study) -> Study:
        """Push the given study to this stage. Optionally set stream.

        Parameters
        ----------
        study: Study
            Send the data in this study

        Raises
        ------
        StudyPushException:
            When pushing the study does not work for some reason

        Returns
        -------
        Study:
            The study after pushing to this stage

        """
        if study.stream not in self.streams:
            raise StudyPushException(f"Stream '{study.stream}' "
                                     f"does not exist in {self}")

        original_stage = study.stage  # keep original for possible rollback

        source = str(original_stage.get_path_for_study(study))
        destination = str(self.get_path_for_stream(study.stream))
        try:
            shutil.move(source, destination)
            study = self.push_study_callback(study)

        except (IDISSendException, PushStudyCallbackException) as e:
            # roll back. move data back where it came from
            shutil.move(destination, source)
            raise StudyPushException(e)
        except (FileNotFoundError, OSError) as e:
            raise StudyPushException(e)

    def push_study_callback(self, study: Study):
        """Function that gets called directly after a study gets pushed to this stage

        Separate callback to house functionality specific to a stage. Separate
        from general functionality in push_study()

        Parameters
        ----------
        study: Study

        Raises
        ------
        PushStudyCallbackException
            When anything goes wrong executing this callback

        Returns
        -------
        Study
            The study after executing the callback

        """
        # Implement specifics in child classes
        return study

    def add_stream(self, stream: Stream):
        """Add stream to this stage

        Raises
        ------
        ValueError
            If already registered
        """
        if stream in self.streams:
            raise ValueError(f'Stream {stream} is already registered with {self}')
        else:
            self._assert_path_for_stream(stream)
            self.streams.append(stream)

    def assert_stream(self, stream: Stream):
        if stream not in self.streams:
            self.add_stream(stream)
            self._assert_path_for_stream(stream)

    def get_path_for_stream(self, stream: Stream) -> Path:
        """Get the folder where data is for this stream """

        return self.path / stream.name

    def _assert_path_for_stream(self, stream: Stream) -> Path:
        """Create path for stream if it does not exist. Should not be called
        directly. Use add_stream() instead """
        path = self.get_path_for_stream(stream)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_path_for_study(self, study: Study) -> Path:
        """Get the folder where data is for this study"""

        return self.get_path_for_stream(study.stream) / study.name

    def get_all_studies(self) -> List[Study]:
        """Get all studies for all streams in this stage
        """

        studies = []
        for stream in self.streams:
            studies += self.get_studies(stream)

        return studies

    def get_studies(self, stream: Stream) -> List[Study]:
        """Get all studies for the given stream

        """
        studies = []
        for folder in [x for x in self.get_path_for_stream(stream).glob('*')]:
            studies.append(Study(name=folder.name,
                                 stream=stream,
                                 stage=self))

        return studies


class Incoming(Stage):
    """A folder where DICOM files are coming into multiple streams
    Assumed directory structure is <stream>/<study>/file

    An Incoming can come up with studies that are deemed complete (see init
    notes below for cooldown)
    """

    def __init__(self, name: str, path: Path, streams: List[Stream],
                 cooldown: int = 5):
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


class PendingAnon(Stage):
    """Stage where data is presented to IDIS for anonymization. Monitors progress
    of anonymization and removes data when anonymization is done or failed.

    Communicates with IDIS to get info
    """
    def __init__(self, name: str, path: Path, streams: List[Stream],
                 idis_connection: IDISConnection):
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
        """

        super(PendingAnon, self).__init__(name=name, path=path, streams=streams)
        self.idis_connection = idis_connection

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
                description=f'Created by idissend for stream '
                            f'{study.stream}',
                pims_keyfile_id=study.stream.idis_project)
        except AnonAPIException as e:
            raise PushStudyCallbackException(e)

        # save job id for this study to check later

        return study


class Trash(Stage):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)"""

    pass


class UnknownStreamException(IDISSendException):
    pass


class StudyPushException(IDISSendException):
    pass


class PushStudyCallbackException(IDISSendException):
    pass
