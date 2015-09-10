from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.serializer import serializer
from nefertari.utils import (
    dictset,
    split_strip,
    )
from .documents import BaseDocument
from .fields import (
    DateField,
    DateTimeField,
    IdField,
    StringField,
    TextField,
    )

__all__ = [
    'BaseDocument',
    'DateField',
    'DateTimeField',
    'IdField',
    'StringField',
    'TextField',
    'setup_database',
    ]


def includeme(config):
    pass


def setup_database(config):
    settings = dictset(config.registry.settings).mget('elasticsearch')
    params = {}
    params['chunk_size'] = settings.get('chunk_size', 500)
    params['hosts'] =[
        dict(host=s[0], port=s[1])
        for s in split_strip(settings['hosts'], ':')
        ]
    if settings.asbool('sniff'):
        params['sniff_on_start'] = True
        params['sniff_on_connection_fail'] = True

    # XXX if this connection has to deal with mongo and sqla objects,
    # then we'll need to use their es serializers instead. should
    # probably clean up that part of the engine interface - there's
    # lots of repeated code, plus other engines shouldn't have to know
    # about es - they should just know how to serialize their
    # documents to JSON.
    connections.create_connection(serializer=serializer, **params)
