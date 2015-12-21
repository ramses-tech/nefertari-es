from argparse import ArgumentParser
import sys
import logging

from pyramid.paster import bootstrap
from pyramid.config import Configurator
from six.moves import urllib

from nefertari.utils import dictset, split_strip, to_dicts
from nefertari.elasticsearch import ES
from nefertari import engine


def main(argv=sys.argv):
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    command = IndexCommand(argv, logger)
    return command.run()


class IndexCommand(object):

    bootstrap = (bootstrap,)
    stdout = sys.stdout
    usage = '%prog config_uri <models'

    def __init__(self, argv, logger):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument(
            '-c', '--config', help='config.ini (required)',
            required=True)
        parser.add_argument(
            '--quiet', help='Quiet mode', action='store_true',
            default=False)
        parser.add_argument(
            '--models',
            help=('Comma-separated list of model names to index '
                  '(required)'),
            required=True)
        parser.add_argument(
            '--params', help='Url-encoded params for each model')
        parser.add_argument(
            '--force',
            help=('Recreate ES mappings and reindex all documents of provided '
                  'models. By default, only documents that are missing from '
                  'index are indexed.'),
            action='store_true',
            default=False)

        # TODO: Use or delete
        # parser.add_argument(
        #     '--chunk',
        #     help=('Index chunk size. If chunk size not provided '
        #           '`elasticsearch.chunk_size` setting is used'),
        #     type=int)

        self._prepare_env(parser, logger)

    def _prepare_env(self, parser, logger):
        self.options = parser.parse_args()
        if not self.options.config:
            return parser.print_help()

        env = self.bootstrap[0](self.options.config)
        registry = env['registry']
        # Include 'nefertari.engine' to setup engines
        config = Configurator(settings=registry.settings)
        config.include('nefertari.engine')

        self.logger = logger
        if not self.options.quiet:
            self.logger.setLevel(logging.INFO)

        self.settings = dictset(registry.settings)

    def _nefertari_es_secondary(self):
        import nefertari_es
        from nefertari import engine
        return engine.secondary is nefertari_es

    def run(self):
        if not self._nefertari_es_secondary():
            self.logger.warning(
                'Nothing to index: nefertari_es is not a secondary '
                'engine')
            return

        model_names = split_strip(self.options.models)
        for model_name in model_names:
            self._index_model(model_name)

    def _index_model(self, model_name):
        self.logger.info('Processing model `{}`'.format(model_name))
        model = engine.get_document_cls(model_name)
        es_model = model._secondary

        params = self.options.params or ''
        params = dict([
            [k, v[0]] for k, v in urllib.parse.parse_qs(params).items()
        ])
        db_queryset = model.get_collection(**params)

        if self.options.force:
            self.logger.info('Recreating `{}` ES mapping'.format(
                model_name))
            # TODO: Recreate mapping
            self.logger.info('Indexing all `{}` documents'.format(
                model_name))
            db_items_data = to_dicts(db_queryset)
        else:
            self.logger.info('Indexing missing `{}` documents'.format(
                model_name))
            pk_field = es_model.pk_field()
            es_items = es_model.get_collection(**params)
            es_pks = [getattr(doc, pk_field) for doc in es_items]
            missing_items = [
                item for item in db_queryset
                if str(getattr(item, pk_field)) not in es_pks]
            db_items_data = to_dicts(missing_items)

        es_docs = [es_model(**data) for data in db_items_data]
        es_model._index_many(es_docs)
