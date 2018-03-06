"""
switchboard.tests.test_models
~~~~~~~~~~~~~~~

:copyright: (c) 2015 Kyle Adams.
:license: Apache License 2.0, see LICENSE for more details.
"""

from datetime import datetime

from nose.tools import assert_equals, assert_true, assert_false
from mock import Mock, patch

from ..manager import SwitchManager
from ..models import Model, VersioningModel, Switch


class TestModel(object):
    def setup(self):
        self.m = Model()

    def teardown(self):
        Model.store.remove()

    def test_get_or_create_get(self):
        self.m.create(key=0, foo='bar')
        defaults = dict(foo='bar')
        instance, created = self.m.get_or_create(defaults=defaults, key=0)
        assert_false(created)
        assert_equals(instance.foo, 'bar')

    def test_get_or_create_create(self):
        defaults = dict(foo='bar')
        instance, created = self.m.get_or_create(defaults=defaults, key=0)
        assert_true(created)
        assert_equals(instance.foo, 'bar')


class TestVersioningModel(object):
    def setup(self):
        self.m = VersioningModel(id='0')

    def teardown(self):
        VersioningModel._versioned_collection().remove()

    def test_diff_fields_added(self):
        self.m.previous_version = lambda: VersioningModel(a=1, b=2)
        self.m.store.get = lambda **kwargs: dict(a=1, b=2, c=3)
        delta = self.m._diff()
        assert_equals(delta['added'], dict(c=3))

    def test_diff_fields_deleted(self):
        self.m.previous_version = lambda: VersioningModel(a=1, b=2)
        self.m.store.get = lambda **kwargs: dict(a=1)
        delta = self.m._diff()
        assert_equals(delta['deleted'], dict(b=2))

    def test_diff_fields_changed(self):
        self.m.previous_version = lambda: VersioningModel(a=1, b=2)
        self.m.store.get = lambda **kwargs: dict(a=1, b=3)
        delta = self.m._diff()
        assert_equals(delta['changed'], dict(b=(2, 3)))

    def test_diff_fields_same(self):
        self.m.previous_version = lambda: VersioningModel(a=1, b=2)
        self.m.store.get = lambda **kwargs: dict(a=1, b=2)
        delta = self.m._diff()
        assert_equals(delta['changed'], dict())
        assert_equals(delta['added'], dict())
        assert_equals(delta['deleted'], dict())

    def test_diff_created(self):
        self.m.previous_version = lambda: None
        self.m.store.get = lambda **kwargs: dict(a=1, b=2)
        delta = self.m._diff()
        assert_equals(delta['changed'], dict())
        assert_equals(delta['added'], dict(a=1, b=2))
        assert_equals(delta['deleted'], dict())

    def test_diff_removed(self):
        self.m.previous_version = lambda: VersioningModel(a=1, b=2)
        self.m.store.get = lambda **kwargs: None
        delta = self.m._diff()
        assert_equals(delta['changed'], dict())
        assert_equals(delta['added'], dict())
        assert_equals(delta['deleted'], dict(a=1, b=2))

    def test_diff_noop(self):
        self.m.previous_version = lambda: None
        self.m.store.get = lambda **kwargs: None
        delta = self.m._diff()
        assert_equals(delta, dict(added={}, deleted={}, changed={}))

    def test_previous_version_new(self):
        c = Mock()
        c.filter.return_value = None
        self.m._versioned_collection = lambda: c
        prev = self.m.previous_version()
        assert_false(hasattr(prev, '_id'))

    def test_previous_version_singlediff(self):
        delta = dict(
            added=dict(a=1, b=2)
        )
        c = Mock()
        c.filter.return_value = [dict(timestamp=datetime.utcnow(),
                                    delta=delta)]
        self.m._versioned_collection = lambda: c
        prev = self.m.previous_version()
        assert_equals(prev.a, 1)
        assert_equals(prev.b, 2)

    def test_previous_version_multidiff(self):
        v1 = dict(
            timestamp=datetime.utcnow(),
            delta=dict(added=dict(a=1, b=2))
        )
        v2 = dict(
            timestamp=datetime.utcnow(),
            delta=dict(changed=dict(b=(2, 3)))
        )
        v3 = dict(
            timestamp=datetime.utcnow(),
            delta=dict(added=dict(c=4))
        )
        v4 = dict(
            timestamp=datetime.utcnow(),
            delta=dict(deleted=dict(a=1))
        )
        c = Mock()
        c.filter.return_value = [v1, v2, v3, v4]
        self.m._versioned_collection = lambda: c
        prev = self.m.previous_version()
        assert_equals(prev.b, 3)
        assert_equals(prev.c, 4)
        assert_false(hasattr(prev, 'a'))


class TestConstant(object):
    def setup(self):
        self.operator = SwitchManager()

    def test_disabled(self):
        assert_true(hasattr(self.operator, 'DISABLED'))
        assert_equals(self.operator.DISABLED, 1)

    def test_selective(self):
        assert_true(hasattr(self.operator, 'SELECTIVE'))
        assert_equals(self.operator.SELECTIVE, 2)

    def test_global(self):
        assert_true(hasattr(self.operator, 'GLOBAL'))
        assert_equals(self.operator.GLOBAL, 3)

    def test_include(self):
        assert_true(hasattr(self.operator, 'INCLUDE'))
        assert_equals(self.operator.INCLUDE, 'i')

    def test_exclude(self):
        assert_true(hasattr(self.operator, 'EXCLUDE'))
        assert_equals(self.operator.EXCLUDE, 'e')


class TestSwitch(object):
    def setup(self):
        self.switch = Switch.create(key='test')

    def teardown(self):
        Switch.store.remove()
        Switch._versioned_collection().remove()

    def test_save_version_changed(self):
        self.switch.key = 'test2'
        self.switch.save()
        assert_equals(
            self.switch.to_bson(), self.switch.store.get(id=self.switch.id))
        vc = self.switch._versioned_collection()
        versions = list(vc.filter(switch_id=self.switch.id))
        assert_true(versions)
        versions.sort(key=lambda x:x['timestamp'])
        version = versions[-1]
        assert_true(version)
        assert_equals(version['delta']['changed']['key'], ('test', 'test2'))
