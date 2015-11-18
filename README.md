# Nefertari-Elasticsearch

Backend and search engine for Nefertari


Multiple engines:

- Create base package that will store code common for all the engines.

- Generate models for secondary engine:
  After model1 is defined, get members&fields of model1, convert all objects from engine1 to objects from engine2. We can already access field init kwargs, so new fields can be instantiated. Not sure about params for other objects and whether other objects except fields exist (probably not). Define model2 as class with one base - BaseDocument from engine and attrs from model1 + re-created fields from engine2. Fields of model1 should be returned by model1 or engine1, because there's no generic way to access model field objects. Model2 will be created by calling type(). Thus model2 will only have one base but will have all the methods and fields of model1. Re-creating fields for model2 can be done by checking whether model1 attribute type is present in engine1 - if True, create and instance of class of the same name from engine2. nefertari.engine could define "setup_database" function that will call functions with the same name from engine1 and engine2.

- Access/query 2nd engine:
  Maybe we can connect two models like - main model would have ".secondary" attribute which will link to generated secondary model and secondary model will have ".primary" attribute which will lead to promary model. In this case collection query can be performed like "Story.secondary.get_collection()".
  UPD: If secondary engine is present and some_param is True - query secondary engine. If secondary engine isn't present or some_param is False - query primary engine. Secondary engine should be queried by default if it is present.

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
