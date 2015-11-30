from nefertari.engine import sync_events


def includeme(config):
    add_sub = config.add_subscriber
    add_sub(handle_item_created, sync_events.ItemCreated)
    add_sub(handle_item_updated, sync_events.ItemUpdated)
    add_sub(handle_item_deleted, sync_events.ItemDeleted)
    add_sub(handle_bulk_updated, sync_events.BulkUpdated)
    add_sub(handle_bulk_deleted, sync_events.BulkDeleted)


def handle_item_created(event):
    pass


def handle_item_updated(event):
    pass


def handle_item_deleted(event):
    pass


def handle_bulk_updated(event):
    pass


def handle_bulk_deleted(event):
    pass
