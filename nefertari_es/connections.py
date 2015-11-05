import json
import logging


import elasticsearch
import six
from nefertari.json_httpexceptions import exception_response


log = logging.getLogger(__name__)


class IndexNotFoundException(Exception):
    pass


class ESHttpConnection(elasticsearch.Urllib3HttpConnection):
    def _catch_index_error(self, response):
        """ Catch and raise index errors which are not critical and thus
        not raised by elasticsearch-py.
        """
        code, headers, raw_data = response
        if not raw_data:
            return
        data = json.loads(raw_data)
        if not data or not data.get('errors'):
            return
        try:
            error_dict = data['items'][0]['index']
            message = error_dict['error']
        except (KeyError, IndexError):
            return
        raise exception_response(400, detail=message)

    def perform_request(self, *args, **kw):
        try:
            if log.level == logging.DEBUG:
                msg = str(args)
                if len(msg) > 512:
                    msg = msg[:300] + '...TRUNCATED...' + msg[-212:]
                log.debug(msg)
            resp = super(ESHttpConnection, self).perform_request(*args, **kw)
        except Exception as e:
            log.error(e.error)
            status_code = e.status_code
            if status_code == 404 and 'IndexMissingException' in e.error:
                raise IndexNotFoundException()
            if status_code == 'N/A':
                status_code = 400
            raise exception_response(
                status_code,
                explanation=six.b(e.error),
                extra=dict(data=e))
        else:
            self._catch_index_error(resp)
            return resp
