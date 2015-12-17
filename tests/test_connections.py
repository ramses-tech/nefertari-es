import logging

import pytest
import six
from mock import patch, Mock
from elasticsearch.exceptions import TransportError
from nefertari.json_httpexceptions import JHTTPBadRequest

from nefertari_es.connections import ESHttpConnection


class TestESHttpConnection(object):

    @patch('nefertari_es.connections.ESHttpConnection._catch_index_error')
    @patch('nefertari_es.connections.log')
    def test_perform_request_debug(self, mock_log, mock_catch):
        mock_log.level = logging.DEBUG
        conn = ESHttpConnection()
        conn.pool = Mock()
        conn.pool.urlopen.return_value = Mock(
            data=six.b('foo'), status=200)
        conn.perform_request('POST', 'http://localhost:9200')
        mock_log.debug.assert_called_once_with(
            "('POST', 'http://localhost:9200')")
        conn.perform_request('POST', 'http://localhost:9200'*200)
        assert mock_catch.called
        assert mock_log.debug.call_count == 2

    def test_catch_index_error_no_data(self):
        conn = ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, None))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_no_data_loaded(self):
        conn = ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, '[]'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_no_errors(self):
        conn = ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, '{"errors":false}'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_not_index_error(self):
        conn = ESHttpConnection()
        try:
            conn._catch_index_error((
                1, 2,
                '{"errors":true, "items": [{"foo": "bar"}]}'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error(self):
        conn = ESHttpConnection()
        with pytest.raises(JHTTPBadRequest):
            conn._catch_index_error((
                1, 2,
                '{"errors":true, "items": [{"index": {"error": "FOO"}}]}'))

    def test_perform_request_exception(self):
        conn = ESHttpConnection()
        conn.pool = Mock()
        conn.pool.urlopen.side_effect = TransportError('N/A', '')
        with pytest.raises(JHTTPBadRequest):
            conn.perform_request('POST', 'http://localhost:9200')
