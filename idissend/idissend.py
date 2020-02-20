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


class Study:
    """A number of files all belonging to the same study """

    def __init__(self, name: str, stream: Stream, files: List[Path]):
        self.name = name
        self.stream = stream
        self.files = files

    def __str__(self):
        return f'{self.stream}:{self.name}'

    def modified_minutes(self) -> float:
        """Yields the number of minutes since modification for each file in study

        Yields instead of return so you can stop checking early

        Yields
        ------
        float
            Number of minutes since file in this study was modified
        """

        for file in self.files:
            delta = datetime.now() - datetime.fromtimestamp(file.stat().st_mtime)
            yield delta.total_seconds() / 60


class IncomingFolder:
    """A folder where DICOM files are coming in in the structure <stream>/<study>"""

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
        self.path = path
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
                files = [x for x in folder.glob('*') if x.is_file()]
                studies.append(Study(folder.name, stream, files))

        if only_cooled:
            studies = [x for x in studies if self.has_cooled_down(x)]

        return studies

    def get_stream_folder(self, stream: Stream) -> Path:
        """Get the folder where data is coming in for this stream """
        return self.path / stream.name

    def has_cooled_down(self, study: Study) -> bool:
        """Check whether files are still coming in for this study

        Considered cooled down if no file was modified less then <cooldown> mins ago
        """

        for age in study.modified_minutes():
            if age <= self.cooldown:
                return False

        return True
