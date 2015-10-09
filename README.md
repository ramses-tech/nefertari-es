# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- finish implementing relationship fields. Also there are
  many other details of relationships that need implementing:
  delete/update triggers, saving related objects when saving an
  instance that has related objects, back refs. Fix endless loop
  when using backref and objects on both sides are connected.

- nesting

- currently GET a collection from the web gives: `to_dict() got
  an unexpected keyword argument '_keys'`. need to track down where this
  is happening.


Later move all es search from other engines and nefertari to this package.
