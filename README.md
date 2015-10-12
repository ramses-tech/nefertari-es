# Nefertari-Elasticsearch

Backend and search engine for Nefertari

## TODO

Some issues to get basic engine working

- finish implementing relationship fields. Also there are
  many other details of relationships that need implementing:
  delete/update triggers. Sync backrefs/relationships.

- nesting - does this only affect returned JSON or does it control how
  related objects are represented in python, e.g. just the id or the
  actual object?

- tests - we need test for idfield and relationship fields.

- is IdField read/write or read-only? also auto-set idfield values
  aren't stored to the db on object creation (cause the id isn't
  available before object save).

Later move all es search from other engines and nefertari to this package.
