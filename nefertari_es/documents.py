from elasticsearch_dsl import DocType


class BaseDocument(DocType):

    def save(self, request=None):
        super(BaseDocument, self).save()
        return self

    def update(self, params, request=None):
        super(BaseDocument, self).update(**params)
        return self

    def delete(self, request=None):
        super(DocType, self).delete()

    def to_dict(self, **kw):
        # XXX do I need to deal with kw?
        # XXX maybe add _type and _pk?
        d = super(BaseDocument, self).to_dict()
        d['_type'] = 'XXX'
        d['id'] = self._id
        return d

    @classmethod
    def pk_field(cls):
        # XXX
        return 'id'

    @classmethod
    def get(cls, id, __raise=False):
        # XXX - need to clarify the interface of this method
        return DocType.get(id=id)

    @classmethod
    def get_resource(cls, id, **kw):
        # XXX - what is the interface of this method?
        return cls.get(id)

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
