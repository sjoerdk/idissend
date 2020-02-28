from pathlib import Path

import factory
from anonapi.responses import JobInfo

from idissend.core import AgedPath, Study, Stream, Person, Stage


class MockAgedPath(AgedPath):
    """An aged path which does not check on disk but just returns a set age value"""

    def __init__(self, path: Path, age: float):
        super(MockAgedPath, self).__init__(path)
        self.age = lambda: age


class MockAgedPathFactory(factory.Factory):
    class Meta:
        model = MockAgedPath

    path = factory.sequence(lambda n: Path(f"mock_path_{n}"))
    age = 10


class PersonFactory(factory.Factory):
    class Meta:
        model = Person

    name = factory.Faker('first_name')
    email = factory.LazyAttribute(lambda a: f'{a.name}@example.com')


class StreamFactory(factory.Factory):
    class Meta:
        model = Stream

    name = factory.sequence(lambda n: f"stream_{n}")
    output_folder = factory.sequence(lambda n: Path(f"mock_path_{n}"))
    idis_project = factory.sequence(lambda n: f"idis_project_{n}")
    pims_key = factory.sequence(lambda n: f"111{n}")
    contact = factory.SubFactory(PersonFactory)


class StageFactory(factory.Factory):
    class Meta:
        model = Stage

    name = factory.sequence(lambda n: Path(f"Mock stage {n}"))
    path = factory.sequence(lambda n: Path(f"mock_path_{n}"))
    streams = factory.List([factory.SubFactory(StreamFactory) for _ in range(3)])


class StudyFactory(factory.Factory):
    class Meta:
        model = Study

    name = factory.sequence(lambda n: f"study_{n}")
    stream = factory.SubFactory(StreamFactory)
    stage = factory.SubFactory(StageFactory)
