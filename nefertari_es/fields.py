import datetime
from dateutil import parser

from elasticsearch_dsl import field
from elasticsearch_dsl.exceptions import ValidationException

IntegerField = field.Integer
IntervalField = field.Integer
SmallIntegerField = field.Integer

StringField = field.String
TextField = field.String
UnicodeField = field.String
UnicodeTextField = field.String

BigIntegerField = field.Long
BooleanField = field.Boolean
FloatField = field.Float
BinaryField = field.Byte
DecimalField = field.Double

# ChoiceField
# PickleField
# TimeField
# Relationship
# IdField
# ForeignKeyField
# ListField


class CustomMappingMixin(object):
    """ Mixin that allows to define custom ES field mapping.

    Set mapping to "_custom_mapping" attribute. Defaults to None, in
    which case default field mapping is used. Custom mapping extends
    default mapping.
    """
    _custom_mapping = None

    def to_dict(self, *args, **kwargs):
        data = super(DictField, self).to_dict(*args, **kwargs)
        if self._custom_mapping is not None:
            data.update(self._custom_mapping)
        return data


class DictField(CustomMappingMixin, field.Object):
    name = 'dict'
    _custom_mapping = {'type': 'object', 'enabled': False}


class DateTimeField(CustomMappingMixin, field.Field):
    name = 'datetime'
    _coerce = True
    _custom_mapping = {'type': 'date', 'format': 'dateOptionalTime'}

    def _to_python(self, data):
        if not data:
            return None
        if isinstance(data, datetime.datetime):
            return data
        try:
            return parser.parse(data)
        except Exception as e:
            raise ValidationException(
                'Could not parse datetime from the value (%r)' % data, e)


class DateField(CustomMappingMixin, field.Date):
    _custom_mapping = {'type': 'date', 'format': 'dateOptionalTime'}


class TimeField(CustomMappingMixin, field.Field):
    name = 'time'
    _coerce = True
    _custom_mapping = {'type': 'date', 'format': 'HH:mm:ss'}

    def _to_python(self, data):
        if not data:
            return None
        if isinstance(data, datetime.time):
            return data
        if isinstance(data, datetime.datetime):
            return data.time()
        try:
            return parser.parse(data).time()
        except Exception as e:
            raise ValidationException(
                'Could not parse time from the value (%r)' % data, e)
