import pytest
from mock import Mock

from nefertari_es.documents import BaseDocument
from nefertari_es.fields import StringField, IntegerField


@pytest.fixture
def simple_model(request):

    class Item(BaseDocument):
        name = StringField()
        price = IntegerField()
        connection = property(Mock())

    return Item
