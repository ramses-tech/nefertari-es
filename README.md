# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- finish implementing relationship fields.

  - implement ondelete and onupdate?

  - improve efficiency of saving and loading of related objects. maybe
    lazy or bulk loading of related objects. Maybe check related
    objects to see if they are dirty before saving them.

  - there is an issue where obj `_id`s are not available until the
    objects are saved. so when saving related objects you can't put
    the backref id in place until after the referring object is
    saved. thus the first time you save an object with backrefs to it,
    the backrefs won't be saved.

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
