"""
switchboard.models
~~~~~~~~~~~~~

:copyright: (c) 2015 Kyle Adams.
:license: Apache License 2.0, see LICENSE for more details.
"""

from datetime import datetime
import logging

from blinker import signal

from .settings import settings
from .store import InMemoryStore


log = logging.getLogger(__name__)

DISABLED = 1
SELECTIVE = 2
GLOBAL = 3
INHERIT = 4

INCLUDE = 'i'
EXCLUDE = 'e'


class Model(object):
    # Store could be lazily overridden by manager.configure().
    store = InMemoryStore()

    post_save = signal('post_save')
    post_delete = signal('post_delete')

    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)

    def to_bson(self):
        # Return a copy so that any subsequent operations don't end up changing
        # this object.
        return self.__dict__.copy()

    def save(self):
        try:
            id_ = self.id
        except AttributeError:
            id_ = None
        previous = self.get(id=id_) if id_ else None
        self.id = self.store.save(self.to_bson())
        self.post_save.send(self)
        return self.id

    def delete(self):
        return self.remove(id=self.id)

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def get(cls, **kwargs):
        data = cls.store.get(**kwargs)
        return cls(**data) if data else None

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        '''
        A port of functionality from the Django ORM. Defaults can be passed in
        if creating a new document is necessary. Keyword args are used to
        lookup the document. Returns a tuple of (object, created), where object
        is the retrieved or created object and created is a boolean specifying
        whether a new object was created.
        '''
        data, created = cls.store.get_or_create(defaults or {}, **kwargs)
        instance = cls(**data)
        if created:
            cls.post_save.send(instance)
        return instance, created

    @classmethod
    def find(cls, **kwargs):
        return [cls(**data) for data in cls.store.filter(**kwargs)]

    @classmethod
    def remove(cls, **kwargs):
        instance = cls.get(**kwargs)
        result = cls.store.remove(**kwargs)
        cls.post_delete.send(instance)
        return result

    @classmethod
    def all(cls):
        return cls.find()

    @classmethod
    def count(cls):
        return cls.store.count()


class Switch(Model):
    """
    Stores information on all switches. Generally handled under the global
    ``switchboard`` namespace.

    ``value`` is stored with by type label, and then by column:

    >>> {
    >>>   namespace: {
    >>>       id: [[INCLUDE, 0, 50], [INCLUDE, 'string']] // 50% of users
    >>>   }
    >>> }
    """

    STATUS_CHOICES = {
        INHERIT: 'Inherit',
        GLOBAL: 'Global',
        SELECTIVE: 'Selective',
        DISABLED: 'Disabled',
    }

    STATUS_LABELS = {
        INHERIT: 'Inherit from parent',
        GLOBAL: 'Active for everyone',
        SELECTIVE: 'Active for conditions',
        DISABLED: 'Disabled for everyone',
    }

    def __init__(self, *args, **kwargs):
        if (
            kwargs and
            hasattr(settings, 'SWITCHBOARD_SWITCH_DEFAULTS') and
            'key' in kwargs and
            'status' not in kwargs
        ):
            key = kwargs['key']
            switch_default = settings.SWITCHBOARD_SWITCH_DEFAULTS.get(key)
            if switch_default is not None:
                is_active = switch_default.get('is_active')
                if is_active is True:
                    kwargs['status'] = GLOBAL
                elif is_active is False:
                    kwargs['status'] = DISABLED
                if not kwargs.get('label'):
                    kwargs['label'] = switch_default.get('label')
                if not kwargs.get('description'):
                    kwargs['description'] = switch_default.get('description')

        self.key = kwargs.get('key')
        self.value = kwargs.get('value', {})
        self.label = kwargs.get('label', '')
        self.date_created = kwargs.get('date_created', datetime.utcnow())
        self.date_modified = kwargs.get('date_modified', datetime.utcnow())
        self.description = kwargs.get('description', '')
        self.status = kwargs.get('status', DISABLED)
        super(Switch, self).__init__(*args, **kwargs)

    def __unicode__(self):
        return u'%s=%s' % (self.key, self.value)

    def get_status_display(self):
        return self.STATUS_CHOICES[self.status]

    def add_condition(self, manager, condition_set, field_name, condition,
                      exclude=False, commit=True):
        """
        Adds a new condition and registers it in the global ``operator`` switch
        manager.

        If ``commit`` is ``False``, the data will not be written to the
        database.

        >>> switch = operator['my_switch'] #doctest: +SKIP
        >>> condition_set_id = condition_set.get_id() #doctest: +SKIP
        >>> switch.add_condition(condition_set_id, 'percent', [0, 50], exclude=False) #doctest: +SKIP
        """
        condition_set = manager.get_condition_set_by_id(condition_set)

        assert isinstance(condition, basestring), 'conditions must be strings'

        namespace = condition_set.get_namespace()

        if namespace not in self.value:
            self.value[namespace] = {}
        if field_name not in self.value[namespace]:
            self.value[namespace][field_name] = []
        if condition not in self.value[namespace][field_name]:
            self.value[namespace][field_name].append([exclude
                                                      and EXCLUDE
                                                      or INCLUDE,
                                                      condition])

        if commit:
            self.save()

    def remove_condition(self, manager, condition_set, field_name, condition,
                         commit=True):
        """
        Removes a condition and updates the global ``operator`` switch manager.

        If ``commit`` is ``False``, the data will not be written to the
        database.

        >>> switch = operator['my_switch'] #doctest: +SKIP
        >>> condition_set_id = condition_set.get_id() #doctest: +SKIP
        >>> switch.remove_condition(condition_set_id, 'percent', [0, 50]) #doctest: +SKIP
        """
        condition_set = manager.get_condition_set_by_id(condition_set)

        namespace = condition_set.get_namespace()

        if namespace not in self.value:
            return

        if field_name not in self.value[namespace]:
            return

        self.value[namespace][field_name] = ([c for c
            in self.value[namespace][field_name] if c[1] != condition])

        if not self.value[namespace][field_name]:
            del self.value[namespace][field_name]

            if not self.value[namespace]:
                del self.value[namespace]

        if commit:
            self.save()

    def clear_conditions(self, manager, condition_set, field_name=None,
                         commit=True):
        """
        Clears conditions given a set of parameters.

        If ``commit`` is ``False``, the data will not be written to the
        database.

        Clear all conditions given a ConditionSet, and a field name:

        >>> switch = operator['my_switch'] #doctest: +SKIP
        >>> condition_set_id = condition_set.get_id() #doctest: +SKIP
        >>> switch.clear_conditions(condition_set_id, 'percent') #doctest: +SKIP

        You can also clear all conditions given a ConditionSet:

        >>> switch = operator['my_switch'] #doctest: +SKIP
        >>> condition_set_id = condition_set.get_id() #doctest: +SKIP
        >>> switch.clear_conditions(condition_set_id) #doctest: +SKIP
        """
        condition_set = manager.get_condition_set_by_id(condition_set)

        namespace = condition_set.get_namespace()

        if namespace not in self.value:
            return

        if not field_name:
            del self.value[namespace]
        elif field_name not in self.value[namespace]:
            return
        else:
            del self.value[namespace][field_name]

        if commit:
            self.save()

    def get_active_conditions(self, manager):
        """
        Returns a generator which yields groups of lists of conditions.

        >>> for label, set_id, field, value, exclude in gargoyle.get_all_conditions(): #doctest: +SKIP
        >>>     print "%(label)s: %(field)s = %(value)s (exclude: %(exclude)s)" % (label, field.label, value, exclude) #doctest: +SKIP
        """
        for condition_set in sorted(manager.get_condition_sets(), key=lambda x: x.get_group_label()):
            ns = condition_set.get_namespace()
            condition_set_id = condition_set.get_id()
            if ns in self.value:
                group = condition_set.get_group_label()
                for name, field in condition_set.fields.iteritems():
                    for value in self.value[ns].get(name, []):
                        try:
                            yield condition_set_id, group, field, value[1], value[0] == EXCLUDE
                        except TypeError:
                            continue

    def get_status_label(self):
        if self.status == SELECTIVE and not self.value:
            status = GLOBAL
        else:
            status = self.status

        return self.STATUS_LABELS[status]

    # TODO: Consolidate to_bson and to_dict; they should be the same. It should
    # be as simple as spitting out __dict__.
    def to_dict(self, manager):
        data = {
            'key': self.key,
            'status': self.status,
            'status_label': self.get_status_label(),
            'label': self.label or self.key.title(),
            'description': self.description,
            'date_modified': self.date_modified,
            'date_created': self.date_created,
            'conditions': [],
        }

        last = None
        actives = self.get_active_conditions(manager)
        for set_id, group, field, value, exclude in actives:
            if not last or last['id'] != set_id:
                if last:
                    data['conditions'].append(last)

                last = {
                    'id': set_id,
                    'label': group,
                    'conditions': []
                }

            last['conditions'].append((field.name, value,
                                       field.display(value), exclude))
        if last:
            data['conditions'].append(last)
        return data
