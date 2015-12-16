import pytest
from mock import Mock, patch
from nefertari.utils import dictset
from nefertari.json_httpexceptions import JHTTPForbidden

from nefertari_es.aggregations import ESAggregator


class TestESAggregator(object):

    class DemoView(object):
        _aggregations_keys = ('test_aggregations',)
        _query_params = dictset()
        _json_params = dictset()

    def test_pop_aggregations_params_query_string(self):
        view = self.DemoView()
        view._query_params = {'test_aggregations.foo': 1, 'bar': 2}
        aggregator = ESAggregator(view)
        params = aggregator.pop_aggregations_params()
        assert params == {'foo': 1}
        assert aggregator._query_params == {'bar': 2}

    def test_pop_aggregations_params_keys_order(self):
        view = self.DemoView()
        view._query_params = {
            'test_aggregations.foo': 1,
            'foobar': 2,
        }
        aggregator = ESAggregator(view)
        aggregator._aggregations_keys = ('test_aggregations', 'foobar')
        params = aggregator.pop_aggregations_params()
        assert params == {'foo': 1}
        assert aggregator._query_params == {'foobar': 2}

    def test_pop_aggregations_params_mey_error(self):
        view = self.DemoView()
        aggregator = ESAggregator(view)
        with pytest.raises(KeyError) as ex:
            aggregator.pop_aggregations_params()
        assert 'Missing aggregation params' in str(ex.value)

    def test_stub_wrappers(self):
        view = self.DemoView()
        view._after_calls = {'index': [1, 2, 3], 'show': [1, 2]}
        aggregator = ESAggregator(view)
        aggregator.stub_wrappers()
        assert aggregator.view._after_calls == {'show': [1, 2], 'index': []}

    @patch('nefertari.elasticsearch.ES')
    def test_aggregate(self, mock_es):
        view = self.DemoView()
        view._auth_enabled = True
        view.Model = Mock(__name__='FooBar')
        aggregator = ESAggregator(view)
        aggregator.check_aggregations_privacy = Mock()
        aggregator.stub_wrappers = Mock()
        aggregator.pop_aggregations_params = Mock(return_value={'foo': 1})
        aggregator._query_params = {'q': '2', 'zoo': 3}
        aggregator.aggregate()
        aggregator.stub_wrappers.assert_called_once_with()
        aggregator.pop_aggregations_params.assert_called_once_with()
        aggregator.check_aggregations_privacy.assert_called_once_with(
            {'foo': 1})
        mock_es.assert_called_once_with('FooBar')
        mock_es().aggregate.assert_called_once_with(
            _aggregations_params={'foo': 1},
            q='2', zoo=3)

    def test_get_aggregations_fields(self):
        params = {
            'min': {'field': 'foo'},
            'histogram': {'field': 'bar', 'interval': 10},
            'aggregations': {
                'my_agg': {
                    'max': {'field': 'baz'}
                }
            }
        }
        result = sorted(ESAggregator.get_aggregations_fields(params))
        assert result == sorted(['foo', 'bar', 'baz'])

    @patch('nefertari.wrappers.apply_privacy')
    def test_check_aggregations_privacy_all_allowed(self, mock_privacy):
        view = self.DemoView()
        view.request = 1
        view.Model = Mock(__name__='Zoo')
        aggregator = ESAggregator(view)
        aggregator.get_aggregations_fields = Mock(return_value=['foo', 'bar'])
        wrapper = Mock()
        mock_privacy.return_value = wrapper
        wrapper.return_value = {'foo': None, 'bar': None}
        try:
            aggregator.check_aggregations_privacy({'zoo': 2})
        except JHTTPForbidden:
            raise Exception('Unexpected error')
        aggregator.get_aggregations_fields.assert_called_once_with({'zoo': 2})
        mock_privacy.assert_called_once_with(1)
        wrapper.assert_called_once_with(
            result={'_type': 'Zoo', 'foo': None, 'bar': None})

    @patch('nefertari.wrappers.apply_privacy')
    def test_check_aggregations_privacy_not_allowed(self, mock_privacy):
        view = self.DemoView()
        view.request = 1
        view.Model = Mock(__name__='Zoo')
        aggregator = ESAggregator(view)
        aggregator.get_aggregations_fields = Mock(return_value=['foo', 'bar'])
        wrapper = Mock()
        mock_privacy.return_value = wrapper
        wrapper.return_value = {'bar': None}
        with pytest.raises(JHTTPForbidden) as ex:
            aggregator.check_aggregations_privacy({'zoo': 2})
        expected = 'Not enough permissions to aggregate on fields: foo'
        assert expected == str(ex.value)
        aggregator.get_aggregations_fields.assert_called_once_with({'zoo': 2})
        mock_privacy.assert_called_once_with(1)
        wrapper.assert_called_once_with(
            result={'_type': 'Zoo', 'foo': None, 'bar': None})

    def view_aggregations_keys_used(self):
        view = self.DemoView()
        view._aggregations_keys = ('foo',)
        assert ESAggregator(view)._aggregations_keys == ('foo',)
        view._aggregations_keys = None
        assert ESAggregator(view)._aggregations_keys == (
            '_aggregations', '_aggs')

    def test_wrap(self):
        view = self.DemoView()
        view.index = Mock(__name__='foo')
        aggregator = ESAggregator(view)
        aggregator.aggregate = Mock(side_effect=KeyError)
        func = aggregator.wrap(view.index)
        func(1, 2)
        aggregator.aggregate.assert_called_once_with()
        view.index.assert_called_once_with(1, 2)
