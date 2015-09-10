from datetime import datetime
from dateutil import parser
from elasticsearch_dsl import (
    Date,
    Field,
    String,
    )


DateField = Date
StringField = String
TextField = String
IdField = String # XXX


class DateTimeField(Field):
    name = 'datetime'
    _coerce = True
    def _to_python(self, data):
        if not data:
            return None
        if isinstance(data, datetime):
            return data
        try:
            return parser.parse(data)
        except Exception as e:
            raise ValidationException(
                'Could not parse datetime from the value (%r)' % data, e
                )
