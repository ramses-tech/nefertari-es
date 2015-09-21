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

    @classmethod
    def get_item(cls, __raise_on_empty=True, **kw):
        """ Get single item and raise exception if not found.

        Exception raising when item is not found can be disabled
        by passing ``__raise_on_empty=False`` in params.

        :returns: Single collection item as an instance of ``cls``.
        """
        params = {}
        for k, v in kw.items():
            if k in cls._doc_type.mapping:
                params[k] = v
        result = cls.search().filter('term', **params).execute()
        if not result and __raise_on_empty:
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
    def get_collection(cls, _count=False, __strict=True, _sort=None,
                       _fields=(), _limit=None, _page=None, _start=None,
                       _query_set=None,
                       **params):
        """ Query collection and return results.

        Notes:
        *   Before validating that only model fields are present in params,
            reserved params, query params and all params starting with
            double underscore are dropped.
        *   Params which have value "_all" are dropped.
        *   When ``_count`` param is used, objects count is returned
            before applying offset and limit.

        :param bool __strict: If True ``params`` are validated to contain
            only fields defined on model, exception is raised if invalid
            fields are present. When False - invalid fields are dropped.
            Defaults to ``True``.
        :param bool _item_request: Indicates whether it is a single item
            request or not. When True and DataError happens on DB request,
            JHTTPNotFound is raised. JHTTPBadRequest is raised when False.
            Defaults to ``False``.
        :param list _sort: Field names to sort results by. If field name
            is prefixed with "-" it is used for "descending" sorting.
            Otherwise "ascending" sorting is performed by that field.
            Defaults to an empty list in which case sorting is not
            performed.
        :param list _fields: Names of fields which should be included
            or excluded from results. Fields to excluded should be
            prefixed with "-". Defaults to an empty list in which
            case all fields are returned.
        :param int _limit: Number of results per page. Defaults
            to None in which case all results are returned.
        :param int _page: Number of page. In conjunction with
            ``_limit`` is used to calculate results offset. Defaults to
            None in which case it is ignored. Params ``_page`` and
            ``_start` are mutually exclusive.
        :param int _start: Results offset. If provided ``_limit`` and
            ``_page`` params are ignored when calculating offset. Defaults
            to None. Params ``_page`` and ``_start`` are mutually
            exclusive. If not offset-related params are provided, offset
            equals to 0.
        :param Query query_set: Existing queryset. If provided, all queries
            are applied to it instead of creating new queryset. Defaults
            to None.
        :param _count: When provided, only results number is returned as
            integer.
        :param _explain: When provided, query performed(SQL) is returned
            as a string instead of query results.
        :param bool __raise_on_empty: When True JHTTPNotFound is raised
            if query returned no results. Defaults to False in which case
            error is just logged and empty query results are returned.

        :returns: Query results as ``elasticsearch_dsl.XXX`` instance.
            May be sorted, offset, limited.
        :returns: Dict of {'field_name': fieldval}, when ``_fields`` param
            is provided.
        :returns: Number of query results as an int when ``_count`` param
            is provided.

        :raises JHTTPNotFound: When ``__raise_on_empty=True`` and no
            results found.
        :raises JHTTPNotFound: When ``_item_request=True`` and
            ``sqlalchemy.exc.DataError`` exception is raised during DB
            query. Latter exception is raised when querying DB with
            an identifier of a wrong type. E.g. when querying Int field
            with a string.
        :raises JHTTPBadRequest: When ``_item_request=False`` and
            ``sqlalchemy.exc.DataError`` exception is raised during DB
            query.
        :raises JHTTPBadRequest: When ``sqlalchemy.exc.InvalidRequestError``
            or ``sqlalchemy.exc.IntegrityError`` errors happen during DB
            query.
        """

        # XXX should we suport query_set?

        return cls.search().execute().hits


    @classmethod
    def get_field_params(cls, field):
        # XXX - seems to be used to provide field init kw args
        return None
