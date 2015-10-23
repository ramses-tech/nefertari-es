import pytest
from mock import patch, Mock
from nefertari.json_httpexceptions import (
    JHTTPBadRequest,
    JHTTPNotFound,
)

from .fixtures import (
    simple_model, id_model, story_model, person_model,
    tag_model, parent_model)
from nefertari_es import documents as docs


class TestBaseDocument(object):

    def test_comparison(self, simple_model):
        item1 = simple_model(name=None)
        item2 = simple_model(name=None)
        assert item1 != item2
        item2.name = '2'
        assert item1 != item2
        item1.name = '1'
        assert item1 != item2
        item1.name = '2'
        assert item1 == item2

    def test_hash(self, simple_model):
        items = set()
        item1 = simple_model(name='1')
        items.add(item1)
        assert item1 in items
        item2 = simple_model(name='asd')
        assert item2 not in items
        item2.name = '1'
        assert item2 in items

    def test_sync_id_field(self, id_model):
        item = id_model()
        assert item.id is None
        item._id = 123
        item._sync_id_field()
        assert item.id == '123'

    def test_setattr_readme_id(self, id_model):
        item = id_model()
        with pytest.raises(AttributeError) as ex:
            item.id = 123
        assert 'id is read-only' in str(ex.value)

    def test_getattr_id_none(self, id_model):
        item = id_model()
        assert item._id is None
        item.meta['id'] = 123
        assert item._id == 123

    @patch('nefertari_es.documents.BaseDocument._load_related')
    def test_getattr_load_rel(self, mock_load, story_model):
        story = story_model()
        story.author
        mock_load.assert_called_once_with('author')

    @patch('nefertari_es.documents.BaseDocument._load_related')
    def test_getattr_raw(self, mock_load, story_model):
        story = story_model(author=1)
        assert story._getattr_raw('author') == 1
        assert not mock_load.called

    @patch('nefertari_es.documents.BaseDocument._load_related')
    def test_unload_related(self, mock_load, parent_model, person_model):
        parent = parent_model()
        parent.children = [person_model(name='123')]
        assert isinstance(parent.children[0], person_model)
        parent._unload_related('children')
        assert parent.children == ['123']

    @patch('nefertari_es.documents.BaseDocument._load_related')
    def test_unload_related_no_ids(self, mock_load, parent_model,
                                   person_model):
        parent = parent_model()
        person = person_model(name=None)
        parent._d_['children'] = [person]
        assert isinstance(parent.children[0], person_model)
        parent._unload_related('children')
        assert parent.children == [person]

    @patch('nefertari_es.documents.BaseDocument._load_related')
    def test_unload_related_no_curr_value(
            self, mock_load, parent_model, person_model):
        parent = parent_model()
        parent._d_['children'] = []
        parent._unload_related('children')
        assert parent.children == []

    def test_load_related(self, parent_model, person_model):
        parent = parent_model()
        parent.children = ['123']
        with patch.object(person_model, 'get_collection') as mock_get:
            mock_get.return_value = ['foo']
            parent._load_related('children')
            mock_get.assert_called_once_with(name=['123'])
            assert parent.children == ['foo']

    def test_load_related_no_items(self, parent_model, person_model):
        parent = parent_model()
        parent.children = ['123']
        with patch.object(person_model, 'get_collection') as mock_get:
            mock_get.return_value = []
            parent._load_related('children')
            mock_get.assert_called_once_with(name=['123'])
            assert parent.children == ['123']

    def test_load_related_no_curr_value(
            self, parent_model, person_model):
        parent = parent_model()
        parent.children = []
        with patch.object(person_model, 'get_collection') as mock_get:
            mock_get.return_value = ['foo']
            parent._load_related('children')
            assert not mock_get.called
            assert parent.children == []

    def test_pk_field(self, simple_model):
        field = simple_model._doc_type.mapping['name']
        field._primary_key = True
        assert simple_model.pk_field() == 'name'

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


@patch('nefertari_es.documents.BaseDocument.search')
class TestGetCollection(object):

    def test_q_search_fields_params(self, mock_search, simple_model):
        result = simple_model.get_collection(
            q='foo', _search_fields='name')
        mock_search.assert_called_once_with()
        mock_search().query.assert_called_once_with(
            'query_string', query='foo', fields=['name'])
        assert result == mock_search().query().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': None,
            'fields': None,
        }

    @patch('nefertari_es.documents.process_limit')
    def test_limit_param(self, mock_proc, mock_search, simple_model):
        mock_proc.return_value = (1, 2)
        result = simple_model.get_collection(_limit=20)
        mock_proc.assert_called_once_with(None, None, 20)
        mock_search.assert_called_once_with()
        mock_search().extra.assert_called_once_with(
            from_=1, size=2)
        assert result == mock_search().extra().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': 1,
            'fields': None,
        }

    @patch('nefertari_es.documents._validate_fields')
    @patch('nefertari_es.documents.process_fields')
    def test_fields_strict_param(
            self, mock_proc, mock_val, mock_search, simple_model):
        mock_proc.return_value = (['name'], ['price'])
        result = simple_model.get_collection(_fields='name,-price')
        mock_proc.assert_called_once_with('name,-price')
        mock_val.assert_called_once_with(simple_model, ['name', 'price'])
        mock_search.assert_called_once_with()
        mock_search().fields.assert_called_once_with(['name'])
        assert result == mock_search().fields().execute().hits
        assert result._nefertari_meta == {
            'total': result.total,
            'start': None,
            'fields': 'name,-price',
        }

    @patch('nefertari_es.documents._cleaned_query_params')
    def test_params_param(self, mock_clean, mock_search, simple_model):
        mock_clean.return_value = {'foo': 1}
        result = simple_model.get_collection(foo=2)
        mock_clean.assert_called_once_with(
            simple_model, {'foo': 2}, True)
        mock_search.assert_called_once_with()
        mock_search().filter.assert_called_once_with(
            'terms', foo=[1])
        assert result == mock_search().filter().execute().hits

    def test_count_param(self, mock_search, simple_model):
        result = simple_model.get_collection(_count=True)
        mock_search.assert_called_once_with()
        assert result == mock_search().count()

    def test_explain_param(self, mock_search, simple_model):
        result = simple_model.get_collection(q='foo', _explain=True)
        mock_search.assert_called_once_with()
        assert result == mock_search().query().to_dict()

    @patch('nefertari_es.documents._validate_fields')
    def test_sort_strict_param(self, mock_val, mock_search, simple_model):
        result = simple_model.get_collection(_sort='name,-price')
        mock_val.assert_called_once_with(simple_model, ['name', 'price'])
        mock_search.assert_called_once_with()
        mock_search().sort.assert_called_once_with(
            'name', '-price')
        assert result == mock_search().sort().execute().hits
