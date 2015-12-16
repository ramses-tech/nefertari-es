import six

from nefertari import wrappers
from nefertari.utils import dictset, validate_data_privacy
from nefertari.json_httpexceptions import JHTTPForbidden


def setup_aggregation(view, aggregator=None):
    """ Wrap `view.index` method with `aggregator`.

    This makes `view.index` to first try to run aggregation and only
    on fail original method is run. Method is wrapped only if it is
    defined and `elasticsearch.enable_aggregations` setting is true.
    """
    from nefertari_es import ESSettings
    if aggregator is None:
        aggregator = Aggregator
    if not ESSettings.asbool('enable_aggregations'):
        return

    index = getattr(view, 'index', None)
    index_defined = index and index != view.not_allowed_action
    if index_defined:
        view.index = aggregator(view).wrap(view.index)


class Aggregator(object):
    """ Provides methods to perform Elasticsearch aggregations.

    Example of using Aggregator:
        >> # Create an instance with view
        >> aggregator = Aggregator(view)
        >> # Replace view.index with wrapped version
        >> view.index = aggregator.wrap(view.index)

    Attributes:
        :_aggregations_keys: Sequence of strings representing name(s) of the
            root key under which aggregations names are defined. Order of keys
            matters - first key found in request is popped and returned. May be
            overriden by defining it on view.

    Examples:
        If _aggregations_keys=('_aggregations',), then query string params
        should look like:
            _aggregations.min_price.min.field=price
    """
    _aggregations_keys = ('_aggregations', '_aggs')

    def __init__(self, view):
        self.view = view
        view_aggregations_keys = getattr(view, '_aggregations_keys', None)
        if view_aggregations_keys:
            self._aggregations_keys = view_aggregations_keys

    def wrap(self, func):
        """ Wrap :func: to perform aggregation on :func: call.

        Should be called with view instance methods.
        """
        @six.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return self.aggregate()
            except KeyError:
                return func(*args, **kwargs)
        return wrapper

    def pop_aggregations_params(self):
        """ Pop and return aggregation params from query string params.

        Aggregation params are expected to be prefixed(nested under) by
        any of `self._aggregations_keys`.
        """
        from nefertari.view import BaseView
        self._query_params = BaseView.convert_dotted(self.view._query_params)

        for key in self._aggregations_keys:
            if key in self._query_params:
                return self._query_params.pop(key)
        else:
            raise KeyError('Missing aggregation params')

    def stub_wrappers(self):
        """ Remove default 'index' after call wrappers and add only
        those needed for aggregation results output.
        """
        self.view._after_calls['index'] = []

    @classmethod
    def get_aggregations_fields(cls, params):
        """ Recursively get values under the 'field' key.

        Is used to get names of fields on which aggregations should be
        performed.
        """
        fields = []
        for key, val in params.items():
            if isinstance(val, dict):
                fields += cls.get_aggregations_fields(val)
            if key == 'field':
                fields.append(val)
        return fields

    def check_aggregations_privacy(self, aggregations_params):
        """ Check per-field privacy rules in aggregations.

        Privacy is checked by making sure user has access to the fields
        used in aggregations.
        """
        fields = self.get_aggregations_fields(aggregations_params)
        fields_dict = dictset.fromkeys(fields)
        fields_dict['_type'] = self.view.Model.__name__

        try:
            validate_data_privacy(self.view.request, fields_dict)
        except wrappers.ValidationError as ex:
            raise JHTTPForbidden(
                'Not enough permissions to aggregate on '
                'fields: {}'.format(ex))

    def aggregate(self):
        """ Perform aggregation and return response. """
        from nefertari.elasticsearch import ES
        aggregations_params = self.pop_aggregations_params()
        if self.view._auth_enabled:
            self.check_aggregations_privacy(aggregations_params)
        self.stub_wrappers()

        return ES(self.view.Model.__name__).aggregate(
            _aggregations_params=aggregations_params,
            **self._query_params)
