from elasticsearch_dsl import Index
from elasticsearch_dsl.document import DocTypeMeta as ESDocTypeMeta

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


class RegisteredDocMixin(type):
    """ Metaclass mixin that registers defined doctypes in
    ``_document_registry``.
    """
    def __new__(cls, name, bases, attrs):
        new_class = super(RegisteredDocMixin, cls).__new__(
            cls, name, bases, attrs)
        _document_registry[new_class.__name__] = new_class
        return new_class


class BackrefGeneratingDocMixin(type):
    """ Metaclass mixin that generates relationship backrefs. """
    def __new__(cls, name, bases, attrs):
        from .fields import Relationship
        new_class = super(BackrefGeneratingDocMixin, cls).__new__(
            cls, name, bases, attrs)

        relationships = new_class._relationships()
        for name in relationships:
            field = new_class._doc_type.mapping[name]
            if not field._backref_kwargs:
                continue
            target_cls = field._doc_class
            backref_kwargs = field._backref_kwargs.copy()
            field_name = backref_kwargs.pop('name')
            backref_kwargs.setdefault('uselist', False)
            backref_field = Relationship(
                new_class.__name__, **backref_kwargs)
            backref_field._back_populates = name
            target_cls._doc_type.mapping.field(field_name, backref_field)
            field._back_populates = field_name

        return new_class


class DocTypeMeta(RegisteredDocMixin,
                  BackrefGeneratingDocMixin,
                  ESDocTypeMeta):
    pass
