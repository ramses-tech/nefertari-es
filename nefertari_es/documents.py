from elasticsearch_dsl import DocType


class BaseDocument(DocType):

    def save(self, request=None):
        super(BaseDocument, self).save()
        return self

    def update(self, params, request=None):
        super(DocType, self).update(**params)
        return self

    def delete(self, request=None):
        super(DocType, self).delete()

    @classmethod
    def pk_field(self):
        # XXX
        return 'id'
