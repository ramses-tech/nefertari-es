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

- implement relationship fields. current plan is to do the joins
  manually the same way that the mongo engine does. but instead of
  storing an dbref, we'll store a tuple of index, document, _id. Also
  we need to implement nesting for fields listed in
  `_nested_relationships`.

- make IdField actually work in terms of fetching the document's
  `_id`. Not sure if this field should allow setting or if it should
  be read-only.


Later move all es search from other engines and nefertari to this package.
