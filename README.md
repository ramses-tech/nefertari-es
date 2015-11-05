# Nefertari-Elasticsearch

Backend and search engine for Nefertari

TODO:

- Add naive versions of missing fields: ChoiceField, PickleField,
  ForeignKeyField, ListField. E.g. If ListField "groups" in nef-example
  has strings, should nefertari-es ListField only support strings?

- Authentication

- Both ramses- and nefertari- examples default branches should work with
  nerfertari-es

- Mock data should load properly


Next tasks:

- Docstrings

- Support 'body' param in ES queries?

- Aggregations

- Multi-collection requests

- Perform bulk operations in chunks

- Use elasticsearch.enable_refresh_query setting

- Rework es.py script in nefertari to work with nefertari-es (indexing
  all documents and missing documents)

- Need a way to generate an es model or mapping given an sql or mongo
  model. this shouldn't be too hard, since the field names are mostly
  the same.

- Try to re-use existing part of the es engine. for example, probably
  can re-use exiting base classes and fields in order to recreate
  something like the existing `get_es_mapping`.

- Need a way to delegate search to es (or other secondary
  engine?). maybe just call `get_collection` on the generated es
  model.

- Will still need hooks in sql and mongo engines to update es models
  when models are changed. it might make sense to change these hooks
  to generate pyramid events. then the es engine can listen for these
  events, thus de-coupling the engines.


Advanced tasks:

- Implement ondelete/onupdate hooks for relationships

- Make relationships sync work on _update_many/_delete_many

- Caching of loaded ES documents

- Unique constraint on PK fields?
