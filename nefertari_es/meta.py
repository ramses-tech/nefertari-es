from elasticsearch_dsl import Index, field
from elasticsearch_dsl.document import DocTypeMeta

from nefertari_es.fields import IdField

# BaseDocument subclasses registry
# maps class names to classes
_document_registry = {}


def create_index(index_name, doc_classes=None):
    """ Create index and add document classes to it.

    Does NOT check whether index already exists.

    :param index_name: Name of index to be created.
    :param doc_classes: Sequence of document classes which should be
        added to created index. Defaults to None, in which case all
        document classes from document registry are added to new index.
    """
    index = Index(index_name)

    if doc_classes is None:
        doc_classes = get_document_classes().values()

    for doc_cls in doc_classes:
        index.doc_type(doc_cls)

    index.create()


def get_document_cls(name):
    """ Get BaseDocument subclass from document registry.

    :param name: String name of BaseDocument subclass to get
    :returns: BaseDocument subclass of name :name:
    :raises KeyError: If document class is not defined
    """
    return _document_registry[name]


def get_document_classes():
    """ Get all defined not abstract document classes. """
    return _document_registry.copy()


class RegisteredDocMeta(DocTypeMeta):
    """ Metaclass that registers defined doctypes in
    ``_document_registry``.
    """
    def __new__(cls, name, bases, attrs):
        new_class = super(RegisteredDocMeta, cls).__new__(
            cls, name, bases, attrs)
        _document_registry[new_class.__name__] = new_class
        return new_class


class IdentifiedDocMeta(RegisteredDocMeta):
    """ Metaclass that adds IdField named "id" to document class
    if primary key field is not already present.
    """
    def __new__(cls, name, bases, attrs):
        for attr, val in attrs.items():
            if (isinstance(val, field.Field) and
                    getattr(val, '_primary_key', None)):
                break
        else:
            attrs['id'] = IdField()

        return super(IdentifiedDocMeta, cls).__new__(
            cls, name, bases, attrs)
