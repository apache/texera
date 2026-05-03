# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest

from core.util.customized_queue.inner import (
    class_inner,
    inner,
    raw_inner,
    static_inner,
)


class TestRawInner:
    def test_returns_the_class_unchanged(self):
        # raw_inner is a no-op decorator preserved for forward-compat with
        # potential changes to default Python inner-class semantics.
        class C:
            pass

        assert raw_inner(C) is C


class TestStaticInner:
    def test_assigns_owner_to_outer_class_at_definition_time(self):
        class Outer:
            @static_inner
            class Inner:
                pass

        # __set_name__ replaces the descriptor with the actual inner class
        # and stamps `owner` so the inner can refer back to its outer class.
        assert Outer.Inner.owner is Outer

    def test_inner_class_is_accessible_directly_on_outer(self):
        class Outer:
            @static_inner
            class Inner:
                @staticmethod
                def hello():
                    return "hi"

        assert Outer.Inner.hello() == "hi"


class TestClassInnerDescriptorGuard:
    @pytest.mark.parametrize("descriptor_method", ["__get__", "__set__", "__del__"])
    def test_rejects_classes_that_implement_descriptor_methods(self, descriptor_method):
        # class_inner refuses to wrap classes that look like descriptors —
        # the `__get__` it installs would conflict with the user-provided one.
        attrs = {descriptor_method: lambda *args, **kwargs: None}
        bad_cls = type("Bad", (object,), attrs)
        with pytest.raises(ValueError, match="descriptors"):
            class_inner(bad_cls)


class TestClassInnerCarriedInheritance:
    def test_subclass_of_outer_gets_a_derived_inner_class(self):
        # When the outer class is subclassed, accessing its inner-class
        # attribute on the subclass produces a *new* inner class that lists
        # the parent's inner class among its bases — this is "carried
        # inheritance". The two inner classes are not the same object and
        # the derived one's owner points at the subclass.
        class Outer:
            @class_inner
            class Inner:
                pass

        class Sub(Outer):
            pass

        derived = Sub.Inner
        assert derived is not Outer.Inner
        assert Outer.Inner in derived.__mro__
        assert derived.owner is Sub
        # Direct access on the original outer is still the original class.
        assert Outer.Inner.owner is Outer


class TestInnerInstanceBinding:
    def test_outer_instance_inner_call_binds_owner_to_that_outer_instance(self):
        # This is the most common @inner usage in Texera: an inner class on
        # an outer object instance, where the inner needs `self.owner` to
        # reach the outer object (e.g. LinkedBlockingMultiQueue's Node /
        # SubQueue / PriorityGroup).
        class Outer:
            def __init__(self, label):
                self.label = label

            @inner
            class Inner:
                def __init__(self, x):
                    self.x = x

        a = Outer("a")
        b = Outer("b")
        a_inner = a.Inner(7)
        b_inner = b.Inner(11)

        assert a_inner.x == 7
        assert b_inner.x == 11
        assert a_inner.owner is a
        assert b_inner.owner is b
        # Instances are independent; binding to one doesn't leak to the other.
        assert a_inner is not b_inner


class TestInnerProperty:
    def test_property_auto_instantiates_inner_on_access(self):
        class Outer:
            @inner.property
            class Counter:
                def __init__(self):
                    self.value = 0

        outer = Outer()
        c = outer.Counter
        # The property short-circuits the constructor signature so accessing
        # the attribute returns a configured instance directly.
        assert c.value == 0
        assert c.owner is outer

    def test_property_returns_a_new_instance_each_access(self):
        # Plain `@inner.property` (without `cached_property`) does not memoize,
        # so two accesses produce two distinct instances. Pin this so the
        # difference between `property` and `cached_property` is preserved.
        class Outer:
            @inner.property
            class Counter:
                def __init__(self):
                    self.value = 0

        outer = Outer()
        first = outer.Counter
        second = outer.Counter
        assert first is not second


class TestInnerCachedProperty:
    def test_cached_property_returns_the_same_instance_on_repeat_access(self):
        # cached_property memoizes the inner instance on the outer object,
        # so subsequent attribute reads return the exact same object.
        class Outer:
            @inner.cached_property
            class Counter:
                def __init__(self):
                    self.value = 0

        outer = Outer()
        first = outer.Counter
        first.value = 42
        second = outer.Counter
        assert first is second
        assert second.value == 42

    def test_cached_property_caches_independently_per_outer_instance(self):
        class Outer:
            @inner.cached_property
            class Counter:
                def __init__(self):
                    self.value = 0

        a = Outer()
        b = Outer()
        a.Counter.value = 1
        b.Counter.value = 2
        assert a.Counter.value == 1
        assert b.Counter.value == 2
