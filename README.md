# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- finish implementing relationship fields.

  - Rework objects dumping for response in to_dict

- tests - we need test relationship fields.


Later move all es search from other engines and nefertari to this
package:

- Move existing es integration out of nefertari (and other engines)
  into es engine

- need a way to generate an es model or mapping given an sql or mongo
  model. this shouldn't be too hard, since the field names are mostly
  the same.

- try to re-use existing part of the es engine. for example, probably
  can re-use exiting base classes and fields in order to recreate
  something like the existing `get_es_mapping`.

- need a way to delegate search to es (or other secondary
  engine?). maybe just call `get_collection` on the generated es
  model.

- will still need hooks in sql and mongo engines to update es models
  when models are changed. it might make sense to change these hooks
  to generate pyramid events. then the es engine can listen for these
  events, thus de-coupling the engines.


Advanced tasks:

- implement ondelete/onupdate hooks for relationships

- make relationships sync work on _update_many/_delete_many

- caching of loaded ES documents
