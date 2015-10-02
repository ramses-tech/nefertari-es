import pytest
from mock import Mock
from elasticsearch_dsl import String, Integer

from nefertari_es.documents import BaseDocument


@pytest.fixture
def simple_model(request):

    class Item(BaseDocument):
        name = String()
        price = Integer()
        connection = property(Mock())

    return Item
