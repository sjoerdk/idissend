"""Core concepts in idissend"""
import logging
import shutil

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
    Responsibilities: A stream is a passive data structure. It should not know about
    where the data it contains is exactly. This is the responsibility of each Stage
    that a stream is in

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
            Final destination of the data. Full UNC path
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

    def get_path(self) -> Path:
        """Full path to the folder that data for this study is in"""
        return self.stage.get_path_for_study(self)

    def get_files(self) -> List[Path]:
        """All files directly in this folder (no recursing)"""
        return [x for x in self.get_path().glob("*") if x.is_file()]

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
        self.logger = logging.getLogger(f'stage "{self.name}"')

    def __str__(self):
        return self.name

    def push_studies(self, studies: List[Study]) -> List[Study]:
        """Insert each study into this stage

        Raises
        ------
        StudyPushException:
            When pushing any study does not work for some reason
        """

        pushed = []
        for study in studies:
            pushed.append(self.push_study(study))

        return pushed

    def push_study(self, study: Study, stream: Stream = None) -> Study:
        """Push the given study to this stage. Optionally set stream.

        Parameters
        ----------
        study: Study
            Send the data in this study
        stream: Stream, optional
            If given, push to this stream. Otherwise use study.stream

        Raises
        ------
        StudyPushException:
            When pushing the study does not work for some reason

        Returns
        -------
        Study:
            The study after pushing to this stage. New object

        """
        self.logger.debug(f"receiving {study}")
        if not stream:
            stream = study.stream

        if stream in self.streams:
            self.assert_path_for_stream(stream)
        else:
            raise StudyPushException(f"Stream '{stream}' " f"does not exist in {self}")

        # create new study that is in this stage
        original_study = study  # keep original for possible rollback
        new_study = Study(name=study.name, stream=stream, stage=self)

        # now move the data from original to new
        try:
            shutil.move(
                str(original_study.get_path()),
                str(new_study.stage.get_path_for_stream(stream)),
            )
            return self.push_study_callback(new_study)

        except (IDISSendException, PushStudyCallbackException) as e:
            self.logger.warning(f"receiving {study} failed: {e}. Rolling back.")
            # roll back. move data back where it came from
            shutil.move(
                str(new_study.get_path()),
                str(original_study.stage.get_path_for_stream(study.stream)),
            )
            raise StudyPushException(e)
        except (FileNotFoundError, OSError) as e:
            self.logger.warning(f"receiving {study} failed: {e}")
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

    def assert_path_for_stream(self, stream: Stream) -> Path:
        """Create path for stream if it does not exist"""
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
            studies += self._get_studies(stream)

        return studies

    def get_studies(self, stream: Stream) -> List[Study]:
        """Get all studies for the given stream

        """
        return self._get_studies(stream=stream)

    def _get_studies(self, stream: Stream) -> List[Study]:
        """hidden method because get_studies itself can be overwritten in parent
        classes

        """
        studies = []
        for folder in [x for x in self.get_path_for_stream(stream).glob("*")]:
            studies.append(Study(name=folder.name, stream=stream, stage=self))

        return studies

    def assert_all_paths(self):
        """Make sure paths to this stage and all stream in it exist

        Useful for initial testing of a stage: you don't have to remember
        the exact paths for expected data"""
        for stream in self.streams:
            self.get_path_for_stream(stream).mkdir(parents=True, exist_ok=True)


class UnknownStreamException(IDISSendException):
    pass


class StudyPushException(IDISSendException):
    pass


class PushStudyCallbackException(IDISSendException):
    pass
