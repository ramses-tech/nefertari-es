# Nefertari-Elasticsearch

Backend and search engine for Nefertari


Multiple engines:

- Fix secondary model generation

- Implement missing handlers to sync data from primary to secondary engine. Test data synchronization.


Next tasks:

- Using multiple engines.

- Need a way to delegate search to es (or other secondary
  engine?). maybe just call `get_collection` on the generated es
  model.

- Need a way to generate an es model or mapping given an sql or mongo
  model. this shouldn't be too hard, since the field names are mostly
  the same.

- Try to re-use existing part of the es engine. for example, probably
  can re-use exiting base classes and fields in order to recreate
  something like the existing `get_es_mapping`.

- Will still need hooks in sql and mongo engines to update es models
  when models are changed. it might make sense to change these hooks
  to generate pyramid events. then the es engine can listen for these
  events, thus de-coupling the engines.

- Aggregations

- Multi-collection requests

- Rework es.py script in nefertari to work with nefertari-es (indexing
  all documents and missing documents)

- Update Ramses to work with multiple engines properly. ramses-example
  should work with nefertari-es and multiple engines.

- Docstrings



Advanced tasks:

- Unique constraint on PK fields?

- Make nefertari-es work with token auth

- Make nefertari-es work wirh nefertari-guards

- Caching of loaded ES documents

- Make relationships sync work on _update_many/_delete_many

- Implement ondelete/onupdate hooks for relationships
