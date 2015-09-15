# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- deal with model `get` and `to_dict` having different symantics in
  elasticsearch-dsl and nefertari. figure out where in nefertari these
  methods are called, and whether its OK to mostly use the elasticsearch-dsl
  symatics.

- figure out how to access es-dsl class registry in order to implement
  `get_document_classes`.

- Translate from `__tablename__` to document class meta info. Right now
  you need to put a Meta class on model classes.

- figure out interface (e.g. expected arguments and return values) for
  `BaseDocument` methods - `get_resource`, `get_collection`.

- Write a bunch of field classes, but only those that make sense for es

- Field validation options

- Field conversion, e.g. pickle, date, etc.

Later move all es search from other engines and nefertari to this package.
