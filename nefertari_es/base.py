from elasticsearch_dsl.document import DocTypeMeta

__all__ = (
    '_document_registry',
    'get_document',
    'RegisteredDocumentMeta',
)


"""
Simple BaseDocument subclasses registry inspired by mongoengine documents
registry.
Stores pairs of {"BaseDocumentName": BaseDocument}.
"""
_document_registry = {}


def get_document(name):
    """ Get BaseDocument subclass from document registry.

    :param name: String name of BaseDocument subclass to get
    :returns: BaseDocument subclass of name :name:
    :raises KeyError: If document class is not defined
    """
    return _document_registry[name]


class RegisteredDocumentMeta(DocTypeMeta):
    """ Metaclass that registers defined doctypes in
    ``_document_registry``.
    """
    def __new__(cls, name, bases, attrs):
        new_class = super(RegisteredDocumentMeta, cls).__new__(
            cls, name, bases, attrs)
        _document_registry[new_class.__name__] = new_class
        return new_class
