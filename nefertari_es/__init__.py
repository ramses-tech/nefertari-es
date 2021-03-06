from __future__ import absolute_import

from elasticsearch_dsl.connections import connections as es_connections
from nefertari.utils import (
    dictset,
    split_strip,
)
from .documents import BaseDocument
from .serializers import JSONSerializer
from .connections import ESHttpConnection
from .meta import (
    get_document_cls,
    get_document_classes,
    create_index,
)
from .fields import (
    IdField,
    IntervalField,
    DictField,
    DateTimeField,
    DateField,
    TimeField,
    IntegerField,
    SmallIntegerField,
    StringField,
    TextField,
    UnicodeField,
    UnicodeTextField,
    BigIntegerField,
    BooleanField,
    FloatField,
    BinaryField,
    DecimalField,
    ReferenceField,
    Relationship,

    ListField,
    ForeignKeyField,
    ChoiceField,
    PickleField,
)


__all__ = [
    'BaseDocument',
    'IdField',
    'IntervalField',
    'DictField',
    'DateTimeField',
    'DateField',
    'TimeField',
    'IntegerField',
    'SmallIntegerField',
    'StringField',
    'TextField',
    'UnicodeField',
    'UnicodeTextField',
    'BigIntegerField',
    'BooleanField',
    'FloatField',
    'BinaryField',
    'DecimalField',
    'ReferenceField',
    'Relationship',
    'setup_database',
    'get_document_cls',
    'get_document_classes',
    'is_relationship_field',
    'get_relationship_cls',

    'ListField',
    'ForeignKeyField',
    'ChoiceField',
    'PickleField',
]


Settings = dictset()


def includeme(config):
    pass


def setup_database(config):
    settings = dictset(config.registry.settings).mget('elasticsearch')
    Settings.update(settings)
    params = {}
    params['chunk_size'] = settings.get('chunk_size', 500)
    params['hosts'] = []
    for hp in split_strip(settings['hosts']):
        h, p = split_strip(hp, ':')
        params['hosts'].append(dict(host=h, port=p))
    if settings.asbool('sniff'):
        params['sniff_on_start'] = True
        params['sniff_on_connection_fail'] = True

    # XXX if this connection has to deal with mongo and sqla objects,
    # then we'll need to use their es serializers instead. should
    # probably clean up that part of the engine interface - there's
    # lots of repeated code, plus other engines shouldn't have to know
    # about es - they should just know how to serialize their
    # documents to JSON.
    conn = es_connections.create_connection(
        serializer=JSONSerializer(),
        connection_class=ESHttpConnection,
        **params)
    setup_index(conn, settings)


def setup_index(conn, settings):
    from nefertari.json_httpexceptions import JHTTPNotFound
    index_name = settings['index_name']
    try:
        index_exists = conn.indices.exists([index_name])
    except JHTTPNotFound:
        index_exists = False
    if not index_exists:
        create_index(index_name)
    else:
        for doc_cls in get_document_classes().values():
            doc_cls._doc_type.index = index_name


def is_relationship_field(field, model_cls):
    return field in model_cls._relationships()


def get_relationship_cls(field, model_cls):
    field_obj = model_cls._doc_type.mapping[field]
    return field_obj._doc_class
