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

from core.storage.model.readonly_virtual_document import ReadonlyVirtualDocument
from core.storage.model.virtual_document import VirtualDocument


class _MinimalDocument(VirtualDocument[int]):
    """The smallest concrete document a backend can supply.

    ``clear`` is the only abstract member of VirtualDocument, so an
    implementation that supports nothing else must still be constructible
    -- every other member falls back to the base "not implemented"
    behaviour. Delegating to ``super().clear()`` also pins that the base
    ``clear`` is a no-op rather than a raiser.
    """

    def __init__(self):
        self.cleared = False

    def clear(self) -> None:
        self.cleared = True
        return super().clear()


class TestVirtualDocumentDefaults:
    """VirtualDocument deliberately gives every read/write method a default
    body that raises, so a backend that cannot reasonably support an
    operation simply does not override it. These tests pin that each
    default raises NotImplementedError (rather than silently returning
    None) and names the method that is missing, which is what a caller
    sees when it reaches for an unsupported operation.
    """

    @pytest.fixture
    def document(self):
        return _MinimalDocument()

    @pytest.mark.parametrize(
        ("method_name", "args"),
        [
            ("get_uri", ()),
            ("get_item", (0,)),
            ("get", ()),
            ("get_range", (0, 1)),
            ("get_after", (0,)),
            ("get_count", ()),
            ("writer", ("writer-1",)),
        ],
    )
    def test_unsupported_operations_raise_with_the_method_name(
        self, document, method_name, args
    ):
        with pytest.raises(NotImplementedError) as exc_info:
            getattr(document, method_name)(*args)

        assert str(exc_info.value) == f"{method_name} method is not implemented"

    def test_read_defaults_are_not_lazy_generators(self, document):
        # get / get_range / get_after are declared as Iterator-returning.
        # If their bodies were generators, calling them would hand back an
        # un-started generator and the failure would surface far from the
        # call site. They must raise eagerly instead.
        for call in (
            lambda: document.get(),
            lambda: document.get_range(0, 1),
            lambda: document.get_after(0),
        ):
            with pytest.raises(NotImplementedError):
                call()

    def test_clear_is_the_only_abstract_member(self, document):
        # A backend only has to implement clear(); everything else is
        # optional. The base clear() body itself is a no-op.
        assert VirtualDocument.__abstractmethods__ == frozenset({"clear"})

        document.clear()

        assert document.cleared is True

    def test_cannot_instantiate_without_clear(self):
        with pytest.raises(TypeError, match="clear"):
            VirtualDocument()

    def test_is_a_readonly_virtual_document(self, document):
        # VirtualDocument extends the read-only abstraction, so anything
        # accepting a ReadonlyVirtualDocument also accepts a writable one.
        assert isinstance(document, ReadonlyVirtualDocument)
