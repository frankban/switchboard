"""Switchboard stores."""

import sqlalchemy as sqla


class _BaseStore(object):

    def get(self, **kwargs):
        items = self.filter(**kwargs)
        return items[0] if items else None

    def get_or_create(self, defaults, **kwargs):
        item = self.get(**kwargs)
        if item:
            return item, False
        item = kwargs
        item.update(defaults)
        id_ = self.save(item)
        return _withid(item, id_), True

    def save(self, item):
        raise NotImplementeds

    def filter(self, **kwargs):
        raise NotImplemented

    def remove(self, **kwargs):
        raise NotImplemented

    def count(self):
        raise NotImplemented

    def versioned(self):
        raise NotImplemented


class InMemoryStore(object):
    """In memory store to be used for development."""

    def __init__(self):
        self._items = {}
        self._versioned = None

    def save(self, item):
        id_ = item.pop('id')
        if not id_:
            id_ = len(self._items)
        self._items[id_] = item

    def filter(self, **kwargs):
        def match(item, filterdict):
            for key, value in filterdict.items():
                if key not in item or item[key] != value:
                    return False
            return True

        id_ = kwargs.pop('id')
        if id_:
            item = self._items.get(id_)
            return [_withid(item, id_)] if item else []
        return [
            _withid(item, id_)
            for id_, item in self._items.items()
            if match(item, kwargs)
        ]

    def remove(self, **kwargs):
        for item in self.filter(**kwargs):
            self._items.pop(item['id'])

    def count(self):
        return len(self._items)

    def versioned(self):
        if not self._versioned:
            self._versioned = self.__class__()
        return self._versioned


class SQLAlchemyStore(object):
    """SQL store to be used in production."""

    def __init__(self, engine, table_name):
        metadata = sqla.MetaData()
        self._table = sqla.Table(table_name, metadata,
            sqla.Column('id', sqla.Integer, primary_key=True),
            sqla.Column('data', sqla.JSON),
        )
        metadata.create_all(engine)
        self._conn = engine.connect()
        self._versioned = None

    def get_or_create(self, defaults, **kwargs):
        with self._conn.begin():
            super().get_or_create(defaults, **kwargs)

    def save(self, item):
        table = self._table
        id_ = item.pop('id')
        op = table.update().where(table.c.id == id_) if id_ else table.insert()
        result = self._conn.execute(op.values(data=item))
        return id_ or result.inserted_primary_key[0]

    def filter(self, **kwargs):
        select = self._match(self._table.select(), kwargs)
        return [_withid(item, id_) for id_, item in self._conn.execute(select)]

    def remove(self, **kwargs):
        delete = self._match(self._table.delete(), kwargs)
        self._conn.execute(delete)

    def count(self):
        select = sqla.select([sqla.func.count()]).select_from(self._table)
        return self._conn.execute(select).scalar()

    def versioned(self):
        if not self._versioned:
            engine = self._conn.engine
            table_name = self._table.name + '_versioned'
            self._versioned = self.__class__(engine, table_name)
        return self._versioned

    def _match(self, op, filterdict):
        table = self._table
        id_ = filterdict.pop('id')
        if id_:
            op = op.where(table.c.id == id_)
        elif filterdict:
            op = op.where(
                table.c.data[key] == value
                for key, value in filterdict.items()
            )
        return op


def _withid(item, id_):
    item['id'] = id_
    return item
