from elasticsearch_dsl.document import DocTypeMeta

# BaseDocument subclasses registry
# maps class names to classes
_document_registry = {}


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


class RegisteredDocumentMeta(DocTypeMeta):
    """ Metaclass that registers defined doctypes in
    ``_document_registry``.
    """
    def __new__(cls, name, bases, attrs):
        new_class = super(RegisteredDocumentMeta, cls).__new__(
            cls, name, bases, attrs)
        _document_registry[new_class.__name__] = new_class
        return new_class
