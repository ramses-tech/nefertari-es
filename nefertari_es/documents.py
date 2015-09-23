from six import (
    add_metaclass,
    string_types,
    )
from elasticsearch_dsl import DocType
from nefertari.json_httpexceptions import (
    JHTTPBadRequest,
    JHTTPNotFound,
    )
from nefertari.utils import (
    process_fields,
    process_limit,
    dictset,
    drop_reserved_params,
    split_strip,
    )
from .meta import RegisteredDocumentMeta


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
        result = cls.get_collection(
            _limit=1, _item_request=True,
            **kw
            )
        if not result:
            if __raise_on_empty:
                msg = "'%s(%s)' resource not found" % (cls.__name__, kw)
                raise JHTTPNotFound(msg)
            return None
        return result[0]

    @classmethod
    def _update_many(cls, items, params, request):
        # XXX
        pass

    @classmethod
    def _delete_many(items, request):
        # XXX
        pass

    @classmethod
    def get_collection(cls, _count=False, __strict=True, _sort=None,
                       _fields=(), _limit=None, _page=None, _start=None,
                       _query_set=None, _item_request=False,
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

        # XXX should we support query_set?
        # XXX should we support _explain?
        # XXX do we need special support for _item_request

        q = cls.search()

        if _limit is not None:
            start, limit = process_limit(_start, _page, _limit)
            q = q.extra(from_=start, size=limit)

        if _fields:
            include, exclude = process_fields(_fields)
            if __strict:
                _validate_fields(include + exclude)
            # XXX partial fields support isn't yet released. for now
            # we just use fields, later we'll add support for excluded fields
            q = q.fields(include)

        if params:
            params = _cleaned_query_params(cls, params, __strict)
            if params:
                q = q.filter('term', **params)

        if _count:
            # XXX use search_type = count? probably more efficient
            return q.execute().hits.total

        if _sort:
            fields = split_strip(_sort)
            if __strict:
                _validate_fields(
                    cls,
                    [f[1:] if f.startswith('-') else f for f in fields]
                    )
            q = q.sort(*fields)

        hits = q.execute().hits
        hits._nefertari_meta = dict(
            total=hits.total,
            start=_start,
            fields=_fields
            )
        return hits

    @classmethod
    def get_field_params(cls, field):
        # XXX - seems to be used to provide field init kw args
        return None



def _cleaned_query_params(cls, params, strict):
    params = {
        key: val for key, val in params.items()
        if not key.startswith('__') and val != '_all'
    }

    # XXX support field__bool and field__in/field__all queries?
    # process_lists(params)
    # process_bools(params)

    if strict:
        _validate_fields(cls, params.keys())
    else:
        field_names = frozenset(cls._doc_type.mapping)
        param_names = frozenset(params.keys())
        invalid_params = param_names.difference(field_names)
        for key in invalid_params:
            del params[key]

    return params


def _validate_fields(cls, field_names):
    valid_names = frozenset(cls._doc_type.mapping)
    names = frozenset(field_names)
    invalid_names = names.difference(valid_names)
    if invalid_names:
         raise JHTTPBadRequest(
            "'%s' object does not have fields: %s" % (
            cls.__name__, ', '.join(invalid_names)))
