from six import (
    add_metaclass,
    string_types,
    )
from elasticsearch_dsl import DocType
from elasticsearch_dsl.utils import AttrList, AttrDict
from elasticsearch import helpers
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
from .meta import BackrefGeneratingDocMeta
from .fields import ReferenceField, IdField


@add_metaclass(BackrefGeneratingDocMeta)
class BaseDocument(DocType):

    _public_fields = None
    _auth_fields = None
    _hidden_fields = None
    _nested_relationships = ()

    def __init__(self, *args, **kwargs):
        super(BaseDocument, self).__init__(*args, **kwargs)
        self._sync_id_field()

    def _sync_id_field(self):
        """ Copy meta["_id"] to IdField. """
        if self.pk_field_type() is IdField:
            pk_field = self.pk_field()
            if not getattr(self, pk_field, None) and self._id is not None:
                self._d_[pk_field] = str(self._id)

    def __setattr__(self, name, value):
        if name == self.pk_field() and self.pk_field_type() == IdField:
            raise AttributeError('{} is read-only'.format(self.pk_field()))
        super(BaseDocument, self).__setattr__(name, value)

    def __getattr__(self, name):
        if name == '_id' and 'id' not in self.meta:
            return None
        return super(BaseDocument, self).__getattr__(name)

    @classmethod
    def _save_relationships(cls, data):
        """ Go through relationship instances and save them, so that
        changes aren't lost, and so that fresh instances get ids
        """
        # XXX should check to see if related objects are dirty before
        # saving, but I don't think that es-dsl keeps track of
        # dirty/clean
        for field in cls._relationships():
            if field not in data:
                continue
            value = data[field]
            if not isinstance(value, (list, AttrList)):
                value = [value]
            return [
                obj.save(relationship=True)
                for obj in value if hasattr(obj, 'save')
                ]

    def _set_backrefs(self):
        #if not self._id:
        #    return
        for name in self._relationships():
            field = self._doc_type.mapping[name]
            backref = field._backref_field_name()
            if not backref:
                continue
            if not name in self:
                continue
            value = self[name]
            if not isinstance(value, (list, AttrList)):
                value = [value]
            for obj in value:
                obj[backref] = self

    @classmethod
    def from_es(cls, hit):
        inst = super(BaseDocument, cls).from_es(hit)
        id = inst[cls.pk_field()]
        if not id in cls._cache:
            cls._cache[id] = inst
        if '_source' not in hit:
            return inst
        doc = hit['_source']

        relationship_fields = cls._relationships()
        for name in relationship_fields:
            if name not in doc:
                continue
            field = cls._doc_type.mapping[name]
            doc_class = field._doc_class
            types = (doc_class, AttrDict)
            data = doc[name]

            single_pk = not field._multi and not isinstance(data, types)
            if single_pk:
                pk_field = doc_class.pk_field()
                inst[name] = doc_class.get_item(**{pk_field: data})

            multi_pk = field._multi and not isinstance(data[0], types)
            if multi_pk:
                pk_field = doc_class.pk_field()
                inst[name] = doc_class.get_collection(**{pk_field: data})

        return inst

    def save(self, request=None, relationship=False):
        self._set_backrefs()
        if not relationship:
            self._save_relationships(self._d_)
        super(BaseDocument, self).save()
        self._sync_id_field()
        return self

    def update(self, params, request=None):
        self._save_relationships(params)
        params = self._flatten_relationships(params)
        super(BaseDocument, self).update(**params)
        return self

    def delete(self, request=None):
        super(BaseDocument, self).delete()

    def to_dict(self, include_meta=False, _keys=None, request=None):
        # avoid serializing backrefs (which leads to endless recursion)
        backrefs = {}
        for name in self._doc_type.mapping:
            field = self._doc_type.mapping[name]
            if isinstance(field, ReferenceField) and field._is_backref:
                if name in self._d_:
                    inst = self._d_[name]
                    backrefs[name] = inst
                    key = inst.pk_field()
                    self._d_[name] = inst[key]

        data = super(BaseDocument, self).to_dict(include_meta=include_meta)

        # put backrefs back
        for name, obj in backrefs.items():
            self._d_[name] = obj

        # XXX DocType and nefertari both expect a to_dict method, but
        # they expect it to act differently. DocType uses to_dict for
        # serialize for saving to es. nefertari uses it to serialize
        # for serving JSON to the client. For now we differentiate by
        # looking for a request argument. If it's present we assume
        # that we're serving JSON to the client, otherwise we assume
        # that we're saving to es
        if request is not None:
            # add some nefertari metadata
            data['_type'] = self.__class__.__name__
            data['_pk'] = str(getattr(self, self.pk_field()))

        # replace referenced instances with their ids when saving to
        # es
        for name in self._relationships():
            if request is not None and name in self._nested_relationships:
                # if we're serving JSON and nesting this field, then
                # don't replace it with its id
                continue
            if include_meta:
                loc = data['_source']
            else:
                loc = data
            if name in loc:
                inst = getattr(self, name)
                field_obj = self._doc_type.mapping[name]
                pk_field = field_obj._doc_class.pk_field()
                if isinstance(inst, (list, AttrList)):
                    loc[name] = [getattr(i, pk_field, None) for i in inst]
                else:
                    loc[name] = getattr(inst, pk_field, None)
        return data

    @classmethod
    def _flatten_relationships(cls, params):
        for name in cls._relationships():
            if name not in params:
                continue
            inst = params[name]
            field_obj = cls._doc_type.mapping[name]
            pk_field = field_obj._doc_class.pk_field()
            if isinstance(inst, (list, AttrList)):
                params[name] = [getattr(i, pk_field, i) for i in inst]
            else:
                params[name] = getattr(inst, pk_field, inst)
        return params

    @classmethod
    def _relationships(cls):
        return [
            name for name in cls._doc_type.mapping
            if isinstance(cls._doc_type.mapping[name], ReferenceField)
            ]

    @classmethod
    def pk_field(cls):
        for name in cls._doc_type.mapping:
            field = cls._doc_type.mapping[name]
            if getattr(field, '_primary_key', False):
                return name
        else:
            raise AttributeError('No primary key field')

    @classmethod
    def pk_field_type(cls):
        pk_field = cls.pk_field()
        return cls._doc_type.mapping[pk_field].__class__

    @classmethod
    def get_item(cls, _raise_on_empty=True, **kw):
        """ Get single item and raise exception if not found.

        Exception raising when item is not found can be disabled
        by passing ``_raise_on_empty=False`` in params.

        :returns: Single collection item as an instance of ``cls``.
        """
        # see if the item is cached
        pk_field = cls.pk_field()
        if list(kw.keys()) == [pk_field]:
            id = kw[pk_field]
            if id in cls._cache:
                return cls._cache[id]

        result = cls.get_collection(
            _limit=1, _item_request=True,
            **kw
            )
        if not result:
            if _raise_on_empty:
                msg = "'%s(%s)' resource not found" % (cls.__name__, kw)
                raise JHTTPNotFound(msg)
            return None
        return result[0]

    @classmethod
    def _update_many(cls, items, params, request=None):
        cls._save_relationships(params)
        params = cls._flatten_relationships(params)
        if not items:
            return

        actions = [item.to_dict(include_meta=True) for item in items]
        for action in actions:
            action.pop('_source')
            action['doc'] = params
        client = items[0].connection
        return _bulk(actions, client, op_type='update', request=request)

    @classmethod
    def _delete_many(cls, items, request=None):
        if not items:
            return

        actions = [item.to_dict(include_meta=True) for item in items]
        client = items[0].connection
        return _bulk(actions, client, op_type='delete', request=request)

    @classmethod
    def get_collection(cls, _count=False, _strict=True, _sort=None,
                       _fields=None, _limit=None, _page=None, _start=None,
                       _query_set=None, _item_request=False, _explain=None,
                       _search_fields=None, q=None, **params):
        """ Query collection and return results.

        Notes:
        *   Before validating that only model fields are present in params,
            reserved params, query params and all params starting with
            double underscore are dropped.
        *   Params which have value "_all" are dropped.
        *   When ``_count`` param is used, objects count is returned
            before applying offset and limit.

        :param bool _strict: If True ``params`` are validated to contain
            only fields defined on model, exception is raised if invalid
            fields are present. When False - invalid fields are dropped.
            Defaults to ``True``.
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
        :param Query _query_set: Existing queryset. If provided, all queries
            are applied to it instead of creating new queryset. Defaults
            to None.
        :param bool _item_request: Indicates whether it is a single item
            request or not. When True and DataError happens on DB request,
            JHTTPNotFound is raised. JHTTPBadRequest is raised when False.
            Defaults to ``False``.
        :param _count: When provided, only results number is returned as
            integer.
        :param _explain: When provided, query performed(SQL) is returned
            as a string instead of query results.
        :param bool _raise_on_empty: When True JHTTPNotFound is raised
            if query returned no results. Defaults to False in which case
            error is just logged and empty query results are returned.
        :param q: Query string to perform full-text search with.
        :param _search_fields: Coma-separated list of field names to use
            with full-text search(q param) to limit fields which are
            searched.

        :returns: Query results as ``elasticsearch_dsl.XXX`` instance.
            May be sorted, offset, limited.
        :returns: Dict of {'field_name': fieldval}, when ``_fields`` param
            is provided.
        :returns: Number of query results as an int when ``_count`` param
            is provided.

        :raises JHTTPNotFound: When ``_raise_on_empty=True`` and no
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
        # see if the items are cached
        pk_field = cls.pk_field()
        if (list(params.keys()) == [pk_field] and _count==False
            and _strict==True and _sort==None and _fields==None
            and _limit==None and _page==None and _start==None
            and _query_set==None and _item_request==False and _explain==None
            and _search_fields==None and q==None):
            ids = params[pk_field]
            if not isinstance(ids, (list, tuple)):
                ids = [ids]
            results = []
            for id in ids:
                if not id in cls._cache:
                    break
                results.append(cls._cache[id])
            else:
                return results

        search_obj = cls.search()

        if _limit is not None:
            _start, limit = process_limit(_start, _page, _limit)
            search_obj = search_obj.extra(from_=_start, size=limit)

        if _fields:
            include, exclude = process_fields(_fields)
            if _strict:
                _validate_fields(cls, include + exclude)
            # XXX partial fields support isn't yet released. for now
            # we just use fields, later we'll add support for excluded fields
            search_obj = search_obj.fields(include)

        if params:
            params = _cleaned_query_params(cls, params, _strict)
            params = _restructure_params(cls, params)
            if params:
                search_obj = search_obj.filter('terms', **params)

        if q is not None:
            query_kw = {'query': q}
            if _search_fields is not None:
                query_kw['fields'] = _search_fields.split(',')
            search_obj = search_obj.query('query_string', **query_kw)

        if _count:
            return search_obj.count()

        if _explain:
            return search_obj.to_dict()

        if _sort:
            sort_fields = split_strip(_sort)
            if _strict:
                _validate_fields(
                    cls,
                    [f[1:] if f.startswith('-') else f for f in sort_fields]
                    )
            search_obj = search_obj.sort(*sort_fields)

        hits = search_obj.execute().hits
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

    @classmethod
    def fields_to_query(cls):
        return set(cls._doc_type.mapping).union({'_id'})


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
        field_names = frozenset(cls.fields_to_query())
        param_names = frozenset(params.keys())
        invalid_params = param_names.difference(field_names)
        for key in invalid_params:
            del params[key]

    return params


def _restructure_params(cls, params):
    pk_field = cls.pk_field()
    if pk_field in params:
        field_obj = cls._doc_type.mapping[pk_field]
        if isinstance(field_obj, IdField):
            params['_id'] = params.pop(pk_field)

    for field, param in params.items():
        if not isinstance(param, list):
            params[field] = [param]
    return params


def _validate_fields(cls, field_names):
    valid_names = frozenset(cls.fields_to_query())
    names = frozenset(field_names)
    invalid_names = names.difference(valid_names)
    if invalid_names:
        raise JHTTPBadRequest(
            "'%s' object does not have fields: %s" % (
            cls.__name__, ', '.join(invalid_names)))


def _bulk(actions, client, op_type='index', request=None):
    for action in actions:
        action['_op_type'] = op_type

    kwargs = {
        'client': client,
        'actions': actions,
    }

    if request is None:
        query_params = {}
    else:
        query_params = request.params.mixed()
    query_params = dictset(query_params)
    # TODO: Use "elasticsearch.enable_refresh_query" setting here
    refresh_enabled = False
    if '_refresh_index' in query_params and refresh_enabled:
        kwargs['refresh'] = query_params.asbool('_refresh_index')

    executed_num, errors = helpers.bulk(**kwargs)
    if errors:
        raise Exception('Errors happened when executing Elasticsearch '
                        'actions: {}'.format('; '.join(errors)))
    return executed_num
