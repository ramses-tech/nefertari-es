# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- are we dealing with `pk_field` correctly? right now we're not
  setting `_id` on the collection, and letting es choose it for us. is
  this a problem? are `get_item` queries slower by our own index than
  by `_id`? Need to clarify this with @artem. I think that things are
  working fine currently, but there may be some reason to actually set
  the `_id` to that same value as the pk field is using.

- finish implementing relationship fields. need to make related
  objects get fetched from es when loading instances that have
  related object. probably should be done in `from_es`. Also there are
  many other details of relationships that need implementing:
  delete/update triggers, saving related objects when saving an
  instance that has related objects, back refs.

- currently GET a collection from the web gives: `to_dict() got
  an unexpected keyword argument '_keys'`. need to track down where this
  is happening.

- make IdField actually work in terms of fetching the document's
  `_id`. Not sure if this field should allow setting or if it should
  be read-only.


Later move all es search from other engines and nefertari to this package.
