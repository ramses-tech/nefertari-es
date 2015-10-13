import pytest
from mock import Mock

from nefertari_es.documents import BaseDocument
from nefertari_es.fields import (
    StringField,
    IntegerField,
    IdField,
    Relationship,
    )


class FakeConnection(object):
    def _get_connection(self, using):
        m = Mock()
        m.index = Mock(return_value={'created': None})
        return m
    def _get_index(self, index=None):
        return Mock()

@pytest.fixture
def simple_model(request):
    class Item(BaseDocument):
        name = StringField(primary_key=True)
        price = IntegerField()
        connection = property(Mock())
    return Item


@pytest.fixture
def person_model():
    class Person(FakeConnection, BaseDocument):
        name = StringField(primary_key=True)

    return Person


@pytest.fixture
def tag_model():
    class Tag(BaseDocument):
        name = StringField(primary_key=True)
    return Tag


@pytest.fixture
def story_model():
    class Story(BaseDocument):
        name = StringField(primary_key=True)
        author = Relationship(document_type='Person', uselist=False)
        tags = Relationship(document_type='Tag', uselist=True)
    return Story


@pytest.fixture
def id_model():
    class Doc(BaseDocument):
        name = StringField()
        id = IdField()
    return Doc

@pytest.fixture
def parent_model():
    class Parent(FakeConnection, BaseDocument):
        name = StringField(primary_key=True)
        children = Relationship(
            document_type='Person',
            uselist=True,
            backref_name='parent'
            )
    return Parent
