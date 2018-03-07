"""
switchboard.tests.test_store
~~~~~~~~~~~~~~~

:copyright: (c) 2018 Canonical Ltd.
:license: Apache License 2.0, see LICENSE for more details.
"""

import datetime
import os
import unittest

import sqlalchemy as sqla

from ..store import (
    InMemoryStore,
    SQLAlchemyStore,
)


_dburl = os.getenv('SWITCHBOARD_DBURL')


class StoreMixin(object):

    def test_get(self):
        # The store can be used to retrieve one item.
        date_created = datetime.datetime.now()
        item0 = {
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item0['id'] = self.store.save(item0)
        item1 = {
            'key': 'bar',
            'status': 47,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item1['id'] = self.store.save(item1)

        tests = (
            ({'id': item1['id']}, item1),
            ({'id': 47}, None),
            ({'key': 'foo'}, item0),
            ({'key': 'foo', 'status': 0}, None),
            ({'key': 'bar', 'date_created': date_created}, item1),
        )
        for query, want_item in tests:
            got_item = self.store.get(**query)
            self.assertEqual(got_item, want_item)

    def test_filter(self):
        # The store can be used to retrieve multiple items.
        date_created = datetime.datetime.now()
        item0 = {
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item0['id'] = self.store.save(item0)
        item1 = {
            'key': 'bar',
            'status': 47,
            'date_created': date_created,
            'value': {'json': 'value2'},
        }
        item1['id'] = self.store.save(item1)
        item2 = {
            'key': 'another key',
            'status': 0,
            'date_created': date_created + datetime.timedelta(1),
            'value': {'json': 'value'},
        }
        item2['id'] = self.store.save(item2)

        tests = (
            ({'id': item2['id']}, [item2]),
            ({'id': 47}, []),
            ({'date_created': date_created}, [item0, item1]),
            ({'key': 'foo', 'status': 0}, []),
            ({}, [item0, item1, item2]),
        )
        for query, want_items in tests:
            got_items = self.store.filter(**query)
            self.assertEqual(got_items, want_items)

    def test_remove(self):
        # Items can be removed.
        date_created = datetime.datetime.now()
        item0 = {
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item0['id'] = self.store.save(item0)
        item1 = {
            'key': 'bar',
            'status': 47,
            'date_created': date_created,
            'value': {'json': 'value2'},
        }
        item1['id'] = self.store.save(item1)
        item2 = {
            'key': 'another key',
            'status': 0,
            'date_created': date_created + datetime.timedelta(1),
            'value': {'json': 'value'},
        }
        item2['id'] = self.store.save(item2)
        item3 = {
            'key': 'spam',
            'status': 0,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item3['id'] = self.store.save(item3)

        # Initially we have 4 items.
        self.assertEqual(self.store.filter(), [item0, item1, item2, item3])

        # Remove a single item.
        self.store.remove(id=item0['id'])
        self.assertEqual(self.store.filter(), [item1, item2, item3])

        # Remove items in bulk.
        self.store.remove(status=0)
        self.assertEqual(self.store.filter(), [item1])

        # Remove all items.
        self.store.remove()
        self.assertEqual(self.store.filter(), [])

    def test_count(self):
        # Items can be counted.
        date_created = datetime.datetime.now()
        self.assertEqual(self.store.count(), 0)
        item0 = {
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item0['id'] = self.store.save(item0)
        item1 = {
            'key': 'bar',
            'status': 47,
            'date_created': date_created,
            'value': {'json': 'value2'},
        }
        item1['id'] = self.store.save(item1)
        self.assertEqual(self.store.count(), 2)

    def test_get_or_create(self):
        # An item is created if not present, or retrieved instead.
        date_created = datetime.datetime.now()

        # Create an item as it doesn't exist.
        item0 = {
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        }
        item, created = self.store.get_or_create(item0, key='foo')
        id_ = item['id']
        self.assertEqual(item, {
            'id': id_,
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        })
        self.assertTrue(created)

        # Get or create again, this time it exists.
        item, created = self.store.get_or_create(item0, key='foo')
        self.assertEqual(item, {
            'id': id_,
            'key': 'foo',
            'status': 42,
            'date_created': date_created,
            'value': {'json': 'value'},
        })
        self.assertFalse(created)
        self.assertEqual(self.store.count(), 1)


class TestInMemoryStore(StoreMixin, unittest.TestCase):

    def setUp(self):
        super(TestInMemoryStore, self).setUp()
        self.store = InMemoryStore()


@unittest.skipUnless(_dburl, 'SWITCHBOARD_DBURL is not defined')
class TestSQLAlchemyStore(StoreMixin, unittest.TestCase):

    def setUp(self):
        super(TestSQLAlchemyStore, self).setUp()
        engine = sqla.create_engine(_dburl, echo=True)
        self.store = SQLAlchemyStore(engine, 'switchboard_testing')

        def cleanup():
            self.store._conn.close()
            self.store._table.drop(engine)

        self.addCleanup(cleanup)
