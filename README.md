# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- deal with model `to_dict` having different symantics in
  elasticsearch-dsl and nefertari. maybe this isn't an issue.

- are we dealing with `pk_field` correctly? right now we're not
  setting `_id` on the collection, and letting es choose it for us. is
  this a problem? are `get_item` queries slower by our own index than
  by `_id`?

- make sure that it's OK not to implement `filter_objects`. Seems like
  this method doen't makes sense for this engine, cause it seems to be
  used to convert es query results to doc instances for other
  engines. probably we need to refactor how this happens when we
  implement query delegation, e.g. having multiple engines defined.

- figure out how to use es bulk api, es-dsl document classes don't
  seem to use it

- implement `?q=query` style of collection querying. need to clarify
  what the search grammar is here.
  https://nefertari.readthedocs.org/en/stable/making_requests.html#query-syntax-for-elasticsearch
  we should support `q` and `_search_fields`. But the code implies
  that we can also provide a `body`.

- Write a bunch of field classes, but only those that make sense for es

- Field validation options

- Field conversion, e.g. pickle, date, etc.

Later move all es search from other engines and nefertari to this package.
