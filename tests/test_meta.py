import pytest
from mock import patch, call

from nefertari_es import meta, documents, fields


class TestMetaHelpers(object):

    @patch('nefertari_es.meta.Index')
    def test_create_index(self, mock_index):
        meta.create_index('index1', ['first', 'second'])
        mock_index.assert_called_once_with('index1')
        mock_index().doc_type.assert_has_calls([
            call('first'), call('second'),
        ], any_order=True)
        mock_index().create.assert_called_once_with()

    @patch('nefertari_es.meta.get_document_classes')
    @patch('nefertari_es.meta.Index')
    def test_test_create_index_no_classes(self, mock_index, mock_get):
        mock_get.return_value = {'foo': 'bar'}
        meta.create_index('index1')
        mock_index.assert_called_once_with('index1')
        mock_index().doc_type.assert_has_calls([
            call('bar')], any_order=True)
        mock_index().create.assert_called_once_with()
        mock_get.assert_called_once_with()


class TestDocumentRegistry(object):
    def test_get_document_cls_key_error(self):
        with pytest.raises(KeyError):
            meta.get_document_cls('MyItem1231')

    def test_get_document_cls(self):
        meta._document_registry = {'MyItem1231': 1}
        assert meta.get_document_cls('MyItem1231') == 1

    def test_get_document_classes(self):
        registry = meta.get_document_classes()
        assert registry == meta._document_registry
        assert registry is not meta._document_registry

    def test_registereddocumentmeta(self):
        from six import add_metaclass

        @add_metaclass(meta.RegisteredDocMeta)
        class MyItem123(object):
            pass

        assert meta._document_registry['MyItem123'] is MyItem123


class TestBackrefGeneratingDocMeta(object):

    def test_backref_generation(self):
        class Tag(documents.BaseDocument):
            name = fields.StringField(primary_key=True)

        with pytest.raises(KeyError):
            Tag._doc_type.mapping['stories']

        class Story(documents.BaseDocument):
            name = fields.StringField(primary_key=True)
            tags = fields.Relationship(
                document='Tag', uselist=True,
                backref_name='stories')

        tag_stories = Tag._doc_type.mapping['stories']
        assert isinstance(tag_stories, fields.ReferenceField)
        assert tag_stories._back_populates == 'tags'
        assert tag_stories._doc_class is Story

        story_tags = Story._doc_type.mapping['tags']
        assert story_tags._back_populates == 'stories'
