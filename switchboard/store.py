"""
switchboard stores

:copyright: (c) 2018 Canonical Ltd.
:license: Apache License 2.0, see LICENSE for more details.
"""

import sqlalchemy as sqla


class _BaseStore(object):
    """Define a common interface for switchboard store backends.

    A store stores items, which are Python dictionaries with at least an "id"
    field holding the primary key.
    """

    def get(self, **kwargs):
        """Retrieve and return an item by looking up the store with kwargs."""
        items = self.filter(**kwargs)
        return items[0] if items else None

    def get_or_create(self, defaults, **kwargs):
        """Retrieve an item if it already exists, create it otherwise.

        The given kwargs are used for looking up the item. The default dict is
        used in conjunction with kwargs for creating the item if not already
        present.
        """
        item = self.get(**kwargs)
        if item:
            return item, False
        item = kwargs
        item.update(defaults)
        id_ = self.save(item)
        return _withid(item, id_), True

    def save(self, item):
        """Add or update the given item to the store."""
        raise NotImplemented

    def filter(self, **kwargs):
        """Retrieve all items matching the given kwargs lookup.

        All items are returned if no kwargs are provided.
        """
        raise NotImplemented

    def remove(self, **kwargs):
        """Remove all items matching the given kwargs lookup.

        The store is emptied if no kwargs are provided.
        """
        raise NotImplemented

    def count(self):
        """Return the number of items in the store."""
        raise NotImplemented


class InMemoryStore(_BaseStore):
    """In memory store to be used for development."""

    def __init__(self):
        self._items = {}

    def save(self, item):
        """Implement _BaseStore.save."""
        id_ = item.pop('id', None)
        if id_ is None:
            id_ = len(self._items)
        self._items[id_] = item
        return id_

    def filter(self, **kwargs):
        """Implement _BaseStore.filter."""
        def match(item, filterdict):
            for key, value in filterdict.items():
                if key not in item or item[key] != value:
                    return False
            return True

        id_ = kwargs.pop('id', None)
        if id_ is not None:
            item = self._items.get(id_)
            return [_withid(item, id_)] if item else []
        return [
            _withid(item, id_)
            for id_, item in self._items.items()
            if match(item, kwargs)
        ]

    def remove(self, **kwargs):
        """Implement _BaseStore.remove."""
        for item in self.filter(**kwargs):
            self._items.pop(item['id'], None)

    def count(self):
        """Implement _BaseStore.count."""
        return len(self._items)


class SQLAlchemyStore(_BaseStore):
    """SQL store to be used in production."""

    def __init__(self, engine, table_name):
        metadata = sqla.MetaData()
        self._table = sqla.Table(table_name, metadata,
            sqla.Column('id', sqla.Integer, primary_key=True),
            sqla.Column('key', sqla.String(100), nullable=False, unique=True),
            sqla.Column('status', sqla.Integer),
            sqla.Column('label', sqla.String(100)),
            sqla.Column('description', sqla.Text),
            sqla.Column('date_created', sqla.DateTime),
            sqla.Column('date_modified', sqla.DateTime),
            sqla.Column('value', sqla.JSON),
        )
        metadata.create_all(engine)
        self._conn = engine.connect()

    def get_or_create(self, defaults, **kwargs):
        """Override _BaseStore.get_or_create."""
        with self._conn.begin():
            return super(SQLAlchemyStore, self).get_or_create(
                defaults, **kwargs)

    def save(self, item):
        """Implement _BaseStore.save."""
        table = self._table
        id_ = item.pop('id', None)
        if id_ is None:
            op = table.insert()
        else:
            op = table.update().where(table.c.id == id_)
        result = self._conn.execute(op.values(**item))
        if id_ is None:
            return result.inserted_primary_key[0]
        return id_

    def filter(self, **kwargs):
        """Implement _BaseStore.filter."""
        select = self._match(self._table.select(), kwargs)
        items = []
        for row in self._conn.execute(select):
            item = dict((k, v) for k, v in row.items() if v is not None)
            items.append(item)
        return items

    def remove(self, **kwargs):
        """Implement _BaseStore.remove."""
        delete = self._match(self._table.delete(), kwargs)
        self._conn.execute(delete)

    def count(self):
        """Implement _BaseStore.count."""
        select = sqla.select([sqla.func.count()]).select_from(self._table)
        return self._conn.execute(select).scalar()

    def _match(self, op, filterdict):
        table = self._table
        id_ = filterdict.pop('id', None)
        if id_ is not None:
            op = op.where(table.c.id == id_)
        elif filterdict:
            op = op.where(sqla.and_(
                getattr(table.c, key) == value
                for key, value in filterdict.items()
            ))
        return op


def _withid(item, id_):
    item['id'] = id_
    return item
