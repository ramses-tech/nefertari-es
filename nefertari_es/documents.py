from six import add_metaclass
from elasticsearch_dsl import DocType
from nefertari.json_httpexceptions import JHTTPNotFound

from .base import RegisteredDocumentMeta


@add_metaclass(RegisteredDocumentMeta)
class BaseDocument(DocType):

    def save(self, request=None):
        super(BaseDocument, self).save()
        return self

    def update(self, params, request=None):
        super(BaseDocument, self).update(**params)
        return self

    def delete(self, request=None):
        super(BaseDocument, self).delete()

    def to_dict(self, **kw):
        # XXX do I need to deal with kw?
        d = super(BaseDocument, self).to_dict()

        # XXX DocType and nefertari both expect a to_dict method, but
        # they expect it to act differently :-(

        # disable these for now - figure out a way to only add them
        # when called by nefertari, not elasticsearch_dsl
        #d['_type'] = self._doc_type.name
        #d['_pk'] = str(getattr(self, self.pk_field()))

        return d

    @classmethod
    def pk_field(cls):
        for name in cls._doc_type.mapping:
            field = cls._doc_type.mapping[name]
            if getattr(field, 'primary_key', False):
                return name
        # XXX default to _id?
        return '_id'

   # XXX DocType and nefertari both expect a get method, but they
   # expect it to act differently - for now we won't use the nefertari
   # get method, using get_resource instead

    @classmethod
    def get_resource(cls, **kw):
        # XXX - what is the interface of this method? I'm guessing
        # that it's a filter search, with also some special kw args
        # should it be restricted to just querying by primary key, or
        # is this a general purpose search method?
        params = {}
        for k, v in kw.items():
            if k in cls._doc_type.mapping:
                params[k] = v
        result = cls.search().filter('term', **params).execute()
        if not result and kw.get('__raise_on_empty', True):
            msg = "'%s(%s)' resource not found" % (cls.__name__, params)
            raise JHTTPNotFound(msg)
        return result[0]

    @classmethod
    def _update_many(cls, items, params, request):
        pass

    @classmethod
    def _delete_many(items, request):
        pass

    @classmethod
    def filter_objects(cls, items):
        # XXX - what kind of filtering is this supposed to do?
        return items

    @classmethod
    def get_collection(cls, **kw):
        # XXX - what sort of kw can be expected? how to massage them?

        # XXX - what is this supposed to return? seems that it should
        # be an object with a `_nefertari_meta` dict, and well as
        # possibly other stuff
        return cls.search().execute().hits

    @classmethod
    def get_field_params(cls, field):
        # XXX - seems to be used to provide field init kw args
        return None
