import pytest
from mock import Mock

from nefertari_es.documents import BaseDocument
from nefertari_es.fields import (
    StringField,
    IntegerField,
    Relationship,
    )


@pytest.fixture
def simple_model(request):

    class Item(BaseDocument):
        name = StringField()
        price = IntegerField()
        connection = property(Mock())

    return Item


@pytest.fixture
def person_model():
    class Person(BaseDocument):
        name = StringField()
    return Person


@pytest.fixture
def tag_model():
    class Tag(BaseDocument):
        name = StringField()
    return Tag


@pytest.fixture
def story_model():
    class Story(BaseDocument):
        title = StringField()
        author = Relationship(document_type='Person', uselist=False)
        tags = Relationship(document_type='Tag', uselist=True)
    return Story
