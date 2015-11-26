# Nefertari-Elasticsearch

Backend and search engine for Nefertari


Multiple engines:

- Access/query 2nd engine:
  * Figure out item access because items in different engines will have different ids. Same applies to any filtering using PK field.

- Sync primary and secondary engines:
  Engines listen to their own DB signals and run handlers when these signals fire. From these signal handlers, Pyramid events are triggered with generic document data (instance, dict). All engines have handlers for all of these Pyramid events which know how to properpy save/update/delete data in that particular engine. Only secondary engines listen to these events. E.g. object is updates in engine1; signal handler of engine1 fires a Pyramid event; engine2 listens to that event and runs event handler; event handler of engine2 updates its own document with the same ID.


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
