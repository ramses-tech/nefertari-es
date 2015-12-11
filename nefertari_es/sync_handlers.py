from nefertari.engine import sync_events


def includeme(config):
    add_sub = config.add_subscriber
    add_sub(handle_item_created, sync_events.ItemCreated)
    add_sub(handle_item_updated, sync_events.ItemUpdated)
    add_sub(handle_item_deleted, sync_events.ItemDeleted)
    add_sub(handle_bulk_updated, sync_events.BulkUpdated)
    add_sub(handle_bulk_deleted, sync_events.BulkDeleted)


def handle_item_created(event):
    item = event.item
    es_model = item.__class__._secondary
    item_data = item.to_dict(_depth=0)
    item_data.pop('_type', None)
    item_data.pop('_version', None)
    item_data.pop('_pk', None)
    es_model(**item_data).save()


def handle_item_updated(event):
    item = event.item
    _update_item(item)


def _update_item(item):
    es_model = item.__class__._secondary
    item_data = item.to_dict(_depth=0)
    item_data.pop('_type', None)
    item_data.pop('_version', None)
    item_pk = item_data.pop('_pk', None)
    pk_field = es_model.pk_field()
    es_item = es_model.get_item(**{pk_field: item_pk})
    es_item.update(item_data)


def handle_item_deleted(event):
    item = event.item
    _delete_item(item)


def _delete_item(item):
    es_model = item.__class__._secondary
    pk_field = es_model.pk_field()
    item_pk = getattr(item, pk_field)
    es_item = es_model.get_item(**{pk_field: item_pk})
    es_item.delete()


def handle_bulk_updated(event):
    items = event.items or []
    for item in items:
        _update_item(item)


def handle_bulk_deleted(event):
    items = event.items or []
    for item in items:
        _delete_item(item)
