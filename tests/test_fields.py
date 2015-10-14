import datetime
from mock import Mock

import pytest
from elasticsearch_dsl.exceptions import ValidationException

from nefertari_es import fields
from .fixtures import (
    id_model,
    story_model,
    tag_model,
    person_model,
    parent_model,
    )

class TestFieldHelpers(object):

    def test_custom_mapping_mixin(self):
        class DummyBase(object):
            def to_dict(self):
                return {'foo': 1, 'bar': 2}

        class DummyField(fields.CustomMappingMixin, DummyBase):
            _custom_mapping = {'foo': 3, 'zoo': 4}

        obj = DummyField()
        assert obj.to_dict() == {'foo': 3, 'bar': 2, 'zoo': 4}


class TestDateTimeField(object):

    def test_to_python_no_data(self):
        obj = fields.DateTimeField()
        assert obj._to_python({}) is None
        assert obj._to_python([]) is None
        assert obj._to_python(None) is None
        assert obj._to_python('') is None

    def test_to_python_datetime(self):
        obj = fields.DateTimeField()
        date = datetime.datetime.now()
        assert obj._to_python(date) is date

    def test_to_python_string_parse(self):
        obj = fields.DateTimeField()
        expected = datetime.datetime(year=2000, month=11, day=12)
        assert obj._to_python('2000-11-12') == expected

    def test_to_python_parse_failed(self):
        obj = fields.DateTimeField()
        with pytest.raises(ValidationException) as ex:
            obj._to_python('asd')
        expected = 'Could not parse datetime from the value'
        assert expected in str(ex.value)


class TestTimeField(object):

    def test_to_python_no_data(self):
        obj = fields.TimeField()
        assert obj._to_python({}) is None
        assert obj._to_python([]) is None
        assert obj._to_python(None) is None
        assert obj._to_python('') is None

    def test_to_python_time(self):
        obj = fields.TimeField()
        time = datetime.datetime.now().time()
        assert obj._to_python(time) is time

    def test_to_python_datetime(self):
        obj = fields.TimeField()
        date = datetime.datetime.now()
        assert obj._to_python(date) == date.time()

    def test_to_python_string_parse(self):
        obj = fields.TimeField()
        expected = datetime.time(17, 40)
        assert obj._to_python('2000-11-12 17:40') == expected

    def test_to_python_parse_failed(self):
        obj = fields.TimeField()
        with pytest.raises(ValidationException) as ex:
            obj._to_python('asd')
        expected = 'Could not parse time from the value'
        assert expected in str(ex.value)


class TestRelationshipField(object):

    def test_to_dict_nested(self, story_model,
                              person_model, tag_model):
        story_model._nested_relationships = ('author', 'tags')
        req = Mock()
        s = story_model(name='Moby Dick')
        assert s.to_dict(request=req) == {
            'name': 'Moby Dick',
            '_pk': 'Moby Dick',
            '_type': 'Story'
            }
        s.author = person_model(name='Melville')
        assert s.to_dict(request=req)['author'] == {'name': 'Melville'}
        s.tags = [tag_model(name='whaling'), tag_model(name='literature')]
        assert s.to_dict(request=req)['tags'] == [
            {'name': 'whaling'}, {'name': 'literature'}
            ]

    def test_to_dict_not_nested(self, story_model,
                                person_model, tag_model):
        req = Mock()
        s = story_model(name='Moby Dick')
        assert s.to_dict(request=req) == {
            'name': 'Moby Dick',
            '_pk': 'Moby Dick',
            '_type': 'Story'
            }
        s.author = person_model(name='Melville')
        assert s.to_dict(request=req)['author'] == 'Melville'
        t1 = tag_model(name='whaling')
        t2 = tag_model(name='literature')
        s.tags = [t1, t2]
        assert s.to_dict(request=req)['tags'] == ['whaling', 'literature']

    def test_to_dict_es(self, story_model, person_model, tag_model):
        s = story_model(name='Moby Dick')
        assert s.to_dict() == {'name': 'Moby Dick'}
        a = person_model(name='Melville')
        s.author = a
        assert s.to_dict()['author'] == 'Melville'
        t1 = tag_model(name='whaling')
        t2 = tag_model(name='literature')
        s.tags = [t1, t2]
        assert s.to_dict()['tags'] == ['whaling', 'literature']

    def test_to_dict_back_ref(self, person_model, parent_model):
        p = parent_model(name='parent-id')
        c = person_model(name='child-id')
        p.children = [c]
        p._set_backrefs()
        assert c.to_dict() == {'name': 'child-id', 'parent': 'parent-id'}

    def test_back_ref(self, person_model, parent_model):
        p = parent_model(name='parent-id')
        c = person_model(name='child-id')
        p.children = [c]
        p._set_backrefs()
        assert p.children[0].parent is p

    def test_load_back_ref(self, person_model, parent_model):
        p = parent_model.from_es(dict(_source=dict(name='parent-id')))
        c = person_model.from_es(dict(_source=dict(name='child-id', parent='parent-id')))
        assert c.parent is p

    def test_load_ref(self, person_model, parent_model):
        c = person_model.from_es(dict(_source=dict(name='child-id')))
        p = parent_model.from_es(dict(_source=dict(name='parent-id', children=['child-id'])))
        assert p.children == [c]

    def test_save_ref(self, person_model, parent_model):
        p = parent_model(name='parent-id')
        c = person_model(name='child-id')
        p.children = [c]
        p.save()
        assert p.children[0].parent is p
        assert c.parent.children[0] is c


class TestIdField(object):

    def test_read_only(self, id_model):
        d = id_model()
        with pytest.raises(AttributeError) as e:
            d.id = 'fail'
        assert str(e.value) == 'id is read-only'

    def test_sync_id(self, id_model):
        d = id_model()
        assert d.id is None

        # simulate a save
        d._id = 'ID'
        d._sync_id_field()

        assert d.id == d._id
