import datetime

import pytest
from elasticsearch_dsl.exceptions import ValidationException

from nefertari_es import fields


class TestFielfHelpers(object):

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
