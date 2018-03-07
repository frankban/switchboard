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
from ..models import Model


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
