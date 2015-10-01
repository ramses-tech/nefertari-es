import pytest
from mock import patch, Mock
from nefertari.json_httpexceptions import (
    JHTTPBadRequest,
    JHTTPNotFound,
    )

from .fixtures import simple_model
from nefertari_es import documents as docs


class TestBaseDocument(object):

    def test_pk_field(self, simple_model):
        field = simple_model._doc_type.mapping['name']
        field.primary_key = True
        assert simple_model.pk_field() == 'name'

    def test_pk_field_default(self, simple_model):
        assert simple_model.pk_field() == '_id'

    def test_get_item_found(self, simple_model):
        simple_model.get_collection = Mock(return_value=['one', 'two'])
        item = simple_model.get_item(foo=1)
        simple_model.get_collection.assert_called_once_with(
            _limit=1, _item_request=True, foo=1)
        assert item == 'one'

    def test_get_item_not_found_not_raise(self, simple_model):
        simple_model.get_collection = Mock(return_value=[])
        item = simple_model.get_item(foo=1, _raise_on_empty=False)
        simple_model.get_collection.assert_called_once_with(
            _limit=1, _item_request=True, foo=1)
        assert item is None

    def test_get_item_not_found_raise(self, simple_model):
        simple_model.get_collection = Mock(return_value=[])
        with pytest.raises(JHTTPNotFound) as ex:
            simple_model.get_item(foo=1)
        assert 'resource not found' in str(ex.value)
        simple_model.get_collection.assert_called_once_with(
            _limit=1, _item_request=True, foo=1)

    @patch('nefertari_es.documents._bulk')
    def test_update_many(self, mock_bulk, simple_model):
        item = simple_model(name='first', price=2)
        simple_model._update_many([item], {'name': 'second'})
        mock_bulk.assert_called_once_with(
            [{'doc': {'name': 'second'}, '_type': 'item'}],
            item.connection, op_type='update', request=None)

    @patch('nefertari_es.documents._bulk')
    def test_delete_many(self, mock_bulk, simple_model):
        item = simple_model(name='first', price=2)
        simple_model._delete_many([item])
        mock_bulk.assert_called_once_with(
            [{'_type': 'item', '_source': {'price': 2, 'name': 'first'}}],
            item.connection, op_type='delete', request=None)

    def test_to_dict(self, simple_model):
        item = simple_model(name='joe', price=42)
        assert item.to_dict() == {'name': 'joe', 'price': 42}
        assert item.to_dict(include_meta=True) == {
            '_source': {'name': 'joe', 'price': 42}, '_type': 'item'
            }

class TestHelpers(object):
    @patch('nefertari_es.documents._validate_fields')
    def test_cleaned_query_params_strict(self, mock_val, simple_model):
        params = {
            'name': 'user12',
            'foobar': 'user12',
            '__id': 2,
            'price': '_all',
        }
        cleaned = docs._cleaned_query_params(simple_model, params, True)
        expected = {'name': 'user12', 'foobar': 'user12'}
        assert cleaned == expected
        mock_val.assert_called_once_with(simple_model, expected.keys())

    def test_cleaned_query_params_not_strict(self, simple_model):
        params = {
            'name': 'user12',
            'foobar': 'user12',
            '__id': 2,
            'price': '_all',
        }
        cleaned = docs._cleaned_query_params(simple_model, params, False)
        assert cleaned == {'name': 'user12'}

    def test_validate_fields_valid(self, simple_model):
        try:
            docs._validate_fields(simple_model, ['name', 'price'])
        except JHTTPBadRequest as ex:
            raise Exception('Unexpected error: {}'.format(str(ex)))

    def test_validate_fields_invalid(self, simple_model):
        with pytest.raises(JHTTPBadRequest) as ex:
            docs._validate_fields(simple_model, ['fofo', 'price'])
        assert 'object does not have fields: fofo' in str(ex.value)

    @patch('nefertari_es.documents.helpers')
    def test_bulk(self, mock_helpers):
        mock_helpers.bulk.return_value = (5, None)
        request = Mock()
        request.params.mixed.return_value = {'_refresh_index': 'true'}
        result = docs._bulk([{'id': 1}], 'foo', 'delete', request)
        mock_helpers.bulk.assert_called_once_with(
            client='foo',
            actions=[{'id': 1, '_op_type': 'delete'}])
        assert result == 5


class TestGetCollection(object):

    def test_q_search_fields_params(self, simple_model):
        result = simple_model.get_collection(
            q='foo', _search_fields='name')
        simple_model.search.assert_called_once_with()
        simple_model.search().query.assert_called_once_with(
            'query_string', query='foo', fields=['name'])
        assert result == simple_model.search().query().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': None,
            'fields': None,
        }

    @patch('nefertari_es.documents.process_limit')
    def test_limit_param(self, mock_proc, simple_model):
        mock_proc.return_value = (1, 2)
        result = simple_model.get_collection(_limit=20)
        mock_proc.assert_called_once_with(None, None, 20)
        simple_model.search.assert_called_once_with()
        simple_model.search().extra.assert_called_once_with(
            from_=1, size=2)
        assert result == simple_model.search().extra().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': 1,
            'fields': None,
        }

    @patch('nefertari_es.documents._validate_fields')
    @patch('nefertari_es.documents.process_fields')
    def test_fields_strict_param(self, mock_proc, mock_val, simple_model):
        mock_proc.return_value = (['name'], ['price'])
        result = simple_model.get_collection(_fields='name,-price')
        mock_proc.assert_called_once_with('name,-price')
        mock_val.assert_called_once_with(simple_model, ['name', 'price'])
        simple_model.search.assert_called_once_with()
        simple_model.search().fields.assert_called_once_with(['name'])
        assert result == simple_model.search().fields().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': None,
            'fields': 'name,-price',
        }

    @patch('nefertari_es.documents._cleaned_query_params')
    def test_params_param(self, mock_clean, simple_model):
        mock_clean.return_value = {'foo': 1}
        result = simple_model.get_collection(foo=2)
        mock_clean.assert_called_once_with(
            simple_model, {'foo': 2}, True)
        simple_model.search.assert_called_once_with()
        simple_model.search().filter.assert_called_once_with(
            'term', foo=1)
        assert result == simple_model.search().filter().execute().hits

    def test_count_param(self, simple_model):
        result = simple_model.get_collection(_count=True)
        simple_model.search.assert_called_once_with()
        assert result == simple_model.search().execute().hits.total

    @patch('nefertari_es.documents._validate_fields')
    def test_sort_strict_param(self, mock_val, simple_model):
        result = simple_model.get_collection(_sort='name,-price')
        mock_val.assert_called_once_with(simple_model, ['name', 'price'])
        simple_model.search.assert_called_once_with()
        simple_model.search().sort.assert_called_once_with(
            'name', '-price')
        assert result == simple_model.search().sort().execute().hits
