import datetime
from dateutil import parser

from elasticsearch_dsl import (
    field,
    DocType,
    )
from elasticsearch_dsl.exceptions import ValidationException
from elasticsearch_dsl.utils import AttrList

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
        data = super(CustomMappingMixin, self).to_dict(*args, **kwargs)
        if self._custom_mapping is not None:
            data.update(self._custom_mapping)
        return data


class BaseFieldMixin(object):
    def __init__(self, *args, **kwargs):
        self._primary_key = kwargs.pop('primary_key', False)
        super(BaseFieldMixin, self).__init__(*args, **kwargs)


class IdField(object):
    """ Descriptor that returns value of document.meta['_id'].

    Because it's not an instance of ``field.Field``, it doesn't
    create field in ES mapping and doesn't allow values set.
    """
    def __get__(self, obj, type=None):
        if hasattr(obj, 'meta') and '_id' in obj.meta:
            return obj.meta['_id']

    def __set__(self, obj, value):
        raise AttributeError("Can't set read-only attribute.")


class IntervalField(BaseFieldMixin, field.Integer):
    """ Custom field that stores `datetime.timedelta` instances.

    Values are stored as seconds in ES and loaded by
    `datetime.timedelta(seconds=<value>) when restoring from ES.
    """
    _coerce = True

    def _to_python(self, data):
        if isinstance(data, int):
            return datetime.timedelta(seconds=data)
        return super(IntervalField, self)._to_python(data)


class DictField(CustomMappingMixin, BaseFieldMixin, field.Object):
    name = 'dict'
    _custom_mapping = {'type': 'object', 'enabled': False}


class DateTimeField(CustomMappingMixin, BaseFieldMixin, field.Field):
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


class DateField(CustomMappingMixin, BaseFieldMixin, field.Date):
    _custom_mapping = {'type': 'date', 'format': 'dateOptionalTime'}


class TimeField(CustomMappingMixin, BaseFieldMixin, field.Field):
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


class IntegerField(BaseFieldMixin, field.Integer):
    pass


class SmallIntegerField(BaseFieldMixin, field.Integer):
    pass


class StringField(BaseFieldMixin, field.String):
    pass


class TextField(BaseFieldMixin, field.String):
    pass


class UnicodeField(BaseFieldMixin, field.String):
    pass


class UnicodeTextField(BaseFieldMixin, field.String):
    pass


class BigIntegerField(BaseFieldMixin, field.Long):
    pass


class BooleanField(BaseFieldMixin, field.Boolean):
    pass


class FloatField(BaseFieldMixin, field.Float):
    pass


class BinaryField(BaseFieldMixin, field.Byte):
    pass


class DecimalField(BaseFieldMixin, field.Double):
    pass


class ReferenceField(CustomMappingMixin, field.Nested):

    _custom_mapping = {'type': 'string'}

    @property
    def _doc_class(self):
        from .meta import get_document_cls
        return get_document_cls(self._doc_class_name)

    @_doc_class.setter
    def _doc_class(self, name):
        self._doc_class_name = name


def Relationship(document_type, uselist=False, nested=True, *args, **kw):
    # XXX deal with backrefs
    # XXX deal with updating, deleting rules

    return ReferenceField(
        multi=uselist,
        doc_class=document_type
        )
