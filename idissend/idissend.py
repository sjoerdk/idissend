# -*- coding: utf-8 -*-
from datetime import datetime
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
    """A single path that incoming data goes through.
    Determines anonymization type and destination

    Notes
    -----
    Data can be sent in to one of multiple streams

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

    def __init__(self, name: str, stream: Stream, stage: 'StageFolder'):
        self.name = name
        self.stream = stream
        self.stage = stage

    def __str__(self):
        return f'{self.stream}:{self.name}'

    @property
    def path(self):
        return self.stage.get_path_for_study(stream=self.stream, study=self)

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


class StageFolder:
    """A distinct step in the pipeline. Contains Studies for different Streams

    A stage is a something like incoming, pending, trash
    """

    def __init__(self, root_path: Path):
        self.root_path = root_path

    def get_path_for_stream(self, stream: Stream) -> Path:
        """Get the folder where data is for this stream """

        return self.root_path / stream.name

    def get_path_for_study(self, stream: Stream, study: Study) -> Path:
        """Get the folder where data is for this stream and study"""

        return self.get_path_for_stream(stream) / study.name


class IncomingFolder(StageFolder):
    """A folder where DICOM files are coming into multiple streams
    Assumed directory structure is <stream>/<study>/file

    An IncomingFolder can come up with studies that are deemed complete (see init
    notes below for cooldown)
    """

    def __init__(self, path: Path, streams=List[Stream], cooldown: int = 5):
        """

        Parameters
        ----------
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
        super(IncomingFolder, self).__init__(root_path=path)
        self.streams = streams
        self.cooldown = cooldown

    def get_studies(self, only_cooled=True) -> List[Study]:
        """Get all studies for all streams in this folder

        Parameters
        ----------
        only_cooled: bool, optional
            If True, return only studies deemed complete after cooldown.
            Otherwise, return all studies. Defaults to True

        """
        studies = []
        for stream in self.streams:
            for folder in [x for x in self.get_stream_folder(stream).glob('*')]:
                studies.append(Study(name=folder.name,
                                     stream=stream,
                                     stage=self))

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


class PendingAnonFolder(StageFolder):
    """A folder where studies are waiting to be anonymized

    Can give information on which studies are waiting, which are done and which are
    probably stale or have a different problem. Can communicate with IDIS to get
    additional info

    """
    pass


class TrashFolder(StageFolder):
    """Where studies are sent after they have been anonymized.

    Can be emptied kind of prudently (Keep as much as possible but also keep
    enough space left)"""

    pass


