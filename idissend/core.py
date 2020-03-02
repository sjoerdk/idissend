"""Core concepts in idissend"""

import shutil

from copy import deepcopy
from datetime import datetime
from idissend.exceptions import IDISSendException
from pathlib import Path
from typing import List


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

    def __init__(
        self,
        name: str,
        output_folder: Path,
        idis_project: str,
        pims_key: str,
        contact: Person,
    ):
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


class IncomingFile:
    """A file which just came in to a folder. Allows checking of age()
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

    def __init__(self, name: str, stream: Stream, stage: "Stage"):
        self.name = name
        self.stream = stream
        self.stage = stage

    def __str__(self):
        return f"{self.stream}:{self.name}"

    @property
    def path(self) -> Path:
        """Full path to the folder that data for this study is in"""
        return self.stage.get_path_for_study(self)

    def get_files(self) -> List[Path]:
        """All files directly in this folder (no recursing)"""
        return [x for x in self.path.glob("*") if x.is_file()]

    def age(self) -> float:
        """Minutes since last modification of any file in this study"""
        return min(IncomingFile(x).age() for x in self.get_files())

    def is_older_than(self, minutes: float):
        """Is age of each file in this study greater than minutes?

        Potentially faster than age() as it does not check all files
        """
        for file in self.get_files():
            if IncomingFile(file).age() <= minutes:
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
    Responsibilities:
    * A stage must know exactly where data is based on stream and study
    * A stage can do internal bookkeeping with the studies it holds
    * A stage must never push or pull studies by itself; moving data away from
      or into a stage is done from the outside.

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
        self.streams = streams

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
        if study.stream in self.streams:
            self._assert_path_for_stream(study.stream)
        else:
            raise StudyPushException(
                f"Stream '{study.stream}' " f"does not exist in {self}"
            )

        original_stage = deepcopy(study.stage)  # keep original for possible rollback
        new_stage = self

        try:
            shutil.move(
                str(original_stage.get_path_for_study(study)),
                str(new_stage.get_path_for_stream(study.stream)),
            )
            study.stage = new_stage
            return self.push_study_callback(study)

        except (IDISSendException, PushStudyCallbackException) as e:
            # roll back. move data back where it came from
            shutil.move(
                str(new_stage.get_path_for_study(study)),
                str(original_stage.get_path_for_stream(study.stream)),
            )
            study.stage = original_stage
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
        for folder in [x for x in self.get_path_for_stream(stream).glob("*")]:
            studies.append(Study(name=folder.name, stream=stream, stage=self))

        return studies


class UnknownStreamException(IDISSendException):
    pass


class StudyPushException(IDISSendException):
    pass


class PushStudyCallbackException(IDISSendException):
    pass
