"""Core concepts in idissend"""
import logging
import random
import shutil
import string

from datetime import datetime

from idissend.exceptions import IDISSendException
from pathlib import Path
from typing import List, Optional


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
        idis_profile_name: str,
        pims_key: str,
        contact: Person,
    ):
        """

        Parameters
        ----------
        name: str
            Name of this stream, doubles as folder study_id
        output_folder: Path
            Final destination of the data. Full UNC path
        idis_profile_name:
            Use the settings in this project for anonymization
        pims_key:
            Use this PIMS project for generating pseudonyms
        contact:
            Who is responsible for collecting this data in the end?
        """

        self.name = name
        self.output_folder = output_folder
        self.idis_profile_name = idis_profile_name
        self.pims_key = pims_key
        self.contact = contact

    def __str__(self):
        return self.name


class IncomingFile:
    """A file which just came in to a folder. Allows checking of age()"""

    def __init__(self, path: Path):
        self.path = path

    def __str__(self):
        return str(self.path)

    def age(self) -> float:
        """Minutes since last modification of this file"""

        delta = datetime.now() - datetime.fromtimestamp(self.path.stat().st_mtime)
        return delta.total_seconds() / 60


class Study:
    """A folder containing files that all belong to the same study"""

    def __init__(self, study_id: str, stream: Stream, stage: "Stage"):
        """

        Parameters
        ----------
        study_id: str
            Unique identifier for this study.
        stream: Stream
            The stream that this study is in
        stage: Stage
            The stage that this study is in
        """
        self.study_id = study_id
        self.stream = stream
        self.stage = stage

    def __str__(self):
        return f"{self.stream}:{self.study_id}"

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
            Human readable study_id for this stage
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

    def push_study(
        self, study: Study, stream: Stream = None, study_id: Optional[str] = None
    ) -> Study:
        """Push the given study to this stage. Optionally set stream.

        Parameters
        ----------
        study: Study
            Send the data in this study
        stream: Optional[Stream]
            If given, push to this stream. Otherwise use study.stream
        study_id: Optional[str]
            If given, give study this ID in this stage. Otherwise use
            study.study_id

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

        if study_id is None:
            study_id = study.study_id

        # create new study that is in this stage
        original_study = study  # keep original for possible rollback
        new_study = Study(study_id=study_id, stream=stream, stage=self)

        # now move the data from original to new
        if new_study.get_path().exists():
            raise StudyPushException(
                f"Study {new_study} at {new_study.get_path()} " f"already exists"
            )
        try:
            shutil.move(
                str(original_study.get_path()), str(new_study.get_path()),
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
        except OSError as e:
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
        """Get the folder where data is for this stream"""

        return self.path / stream.name

    def assert_path_for_stream(self, stream: Stream) -> Path:
        """Create path for stream if it does not exist"""
        path = self.get_path_for_stream(stream)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_path_for_study(self, study: Study) -> Path:
        """Get the folder where data is for this study"""

        return self.get_path_for_stream(study.stream) / study.study_id

    def get_all_studies(self) -> List[Study]:
        """Get all studies for all streams in this stage"""

        studies = []
        for stream in self.streams:
            studies += self._get_studies(stream)

        return studies

    def get_studies(self, stream: Stream) -> List[Study]:
        """Get all studies for the given stream"""
        return self._get_studies(stream=stream)

    def _get_studies(self, stream: Stream) -> List[Study]:
        """Hidden method because get_studies itself can be overwritten in parent
        classes
        """
        studies = []
        for folder in [x for x in self.get_path_for_stream(stream).glob("*")]:
            studies.append(Study(study_id=folder.name, stream=stream, stage=self))

        return studies

    def assert_all_paths(self):
        """Make sure paths to this stage and all stream in it exist

        Useful for initial testing of a stage: you don't have to remember
        the exact paths for expected data
        """
        for stream in self.streams:
            self.get_path_for_stream(stream).mkdir(parents=True, exist_ok=True)


def random_string(k: int) -> str:
    """A random string of uppercase letters + digits, like '2KVDU2D9'

    Parameters
    ----------
    k: int
        string length
    """
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


class UnknownStreamException(IDISSendException):
    pass


class StudyPushException(IDISSendException):
    pass


class PushStudyCallbackException(IDISSendException):
    pass
