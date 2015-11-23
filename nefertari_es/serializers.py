from nefertari.engine.common import (
    JSONEncoderMixin as NefEncoderMixin)

from elasticsearch_dsl.serializer import AttrJSONSerializer


class JSONSerializer(NefEncoderMixin, AttrJSONSerializer):
    pass
