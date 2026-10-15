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

"""Structural spec for AsyncRPCHandlerInitializer.

The class body is `pass`: its mixin list IS the wiring between the generated
WorkerService and the Python handlers, and nothing else covers it --
test_async_rpc_server.py substitutes a stub. Expectations are derived from the
generated code, so a new proto RPC fails here until it gets a handler or is
recorded in UNIMPLEMENTED_RPCS.

The mixin checks assert a project convention -- flat handlers, one RPC each,
wired directly into the bases -- not a language rule; revisit them if that
design ever changes.
"""

import importlib
import inspect
import pkgutil
import typing
from typing import NamedTuple

import grpclib.const
import pytest

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.rpc.async_rpc_handler_initializer import (
    AsyncRPCHandlerInitializer,
)
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models.internal_queue import InternalQueue
from core.util import get_one_of, set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlError,
    ControlInvocation,
    ControlRequest,
    ControlReturn,
    EmptyRequest,
    EmptyReturn,
    ErrorLanguage,
    ReturnInvocation,
    WorkerServiceBase,
)

# Knowingly left on the generated UNIMPLEMENTED stub: checkpointing is
# Scala-only, and flush_network_buffer is a Scala-side network concern (not
# checkpoint-related, despite travelling with them). The one hand-maintained
# fact in this spec.
UNIMPLEMENTED_RPCS = frozenset(
    {
        "finalize_checkpoint",
        "flush_network_buffer",
        "prepare_checkpoint",
        "retrieve_state",
    }
)

# Scanned on disk to catch handler classes the MRO cannot see.
HANDLER_PACKAGE = "core.architecture.handlers.control"


class _Rpc(NamedTuple):
    """One `__mapping__` entry; RPCS keys it by dispatched name ("assign_port")."""

    # Bare proto name in ControlInvocation.method_name, e.g. "AssignPort".
    wire_name: str
    # The key AsyncRPCServer stores and receive() looks up.
    lookup_key: str
    handler: grpclib.const.Handler


def _make_initializer() -> AsyncRPCHandlerInitializer:
    """Real initializer over a sentinel context: ControlHandler.__init__ only
    stores it, and a bare object() makes stray attribute access fail loudly."""
    return AsyncRPCHandlerInitializer(object())


def _collect_rpcs() -> dict[str, _Rpc]:
    """Decompose the real initializer's `__mapping__` into _Rpc entries.

    The dispatched name comes from `func.__name__` ("__rpc_<name>", unmangled
    on the function object). If betterproto's codegen ever renames that,
    test_mapping_covers_every_declared_worker_service_rpc fails first.
    """
    rpcs = {}
    for path, handler in _make_initializer().__mapping__().items():
        name = handler.func.__name__.removeprefix("__rpc_")
        wire_name = path.split("/")[-1]
        rpcs[name] = _Rpc(wire_name, wire_name.lower(), handler)
    return rpcs


def _declared_rpc_names() -> set[str]:
    """The RPC coroutines WorkerServiceBase declares, from the class itself."""
    return {
        name
        for name, value in vars(WorkerServiceBase).items()
        if not name.startswith("_") and inspect.iscoroutinefunction(value)
    }


def _defining_class(rpc_name: str) -> type | None:
    """The MRO class supplying `rpc_name`: WorkerServiceBase means "nobody
    overrode the UNIMPLEMENTED stub"; None means the derivation broke."""
    return next(
        (cls for cls in AsyncRPCHandlerInitializer.__mro__ if rpc_name in vars(cls)),
        None,
    )


def _handler_mixins() -> list[type]:
    """The ControlHandler subclasses mixed into the initializer."""
    return [
        cls
        for cls in AsyncRPCHandlerInitializer.__mro__
        if issubclass(cls, ControlHandler)
        and cls is not ControlHandler
        and cls is not AsyncRPCHandlerInitializer
    ]


def _oneof_member_types(message: type) -> set[type]:
    """Message types a betterproto oneof can carry (fields with a `.group`).

    get_type_hints resolves the generated string annotations, so this compares
    classes, not spellings.
    """
    hints = typing.get_type_hints(message)
    return {
        hints[name]
        for name, meta in message._betterproto.meta_by_field_name.items()
        if meta.group
    }


def _handler_classes_on_disk() -> dict[str, type]:
    """Every RPC-serving ControlHandler subclass under HANDLER_PACKAGE.

    The one view not derived from the MRO -- the only one that can see a
    handler nobody wired in. walk_packages so subpackages are not blind spots.
    """
    package = importlib.import_module(HANDLER_PACKAGE)
    found = {}
    for module_info in pkgutil.walk_packages(package.__path__, f"{HANDLER_PACKAGE}."):
        module = importlib.import_module(module_info.name)
        for value in vars(module).values():
            if (
                inspect.isclass(value)
                and issubclass(value, ControlHandler)
                and value is not ControlHandler
                # Only RPC-serving classes need wiring; a shared base would
                # otherwise force a pointless mixin or an exclusion list.
                and not RPCS.keys().isdisjoint(vars(value))
            ):
                # Keyed by identity: dedupes re-exports without hiding a
                # foreign handler re-exported into the package.
                found[f"{value.__module__}.{value.__qualname__}"] = value
    return found


RPCS = _collect_rpcs()

# Derived, not listed: a newly generated RPC joins on its own and fails
# until a handler exists.
SERVED_RPCS = sorted(name for name in RPCS if name not in UNIMPLEMENTED_RPCS)


class TestGeneratedRpcSurface:
    """Guards the derivation the rest of the spec relies on."""

    def test_mapping_covers_every_declared_worker_service_rpc(self):
        # Also proves the `__rpc_` strip in _collect_rpcs is sound.
        assert set(RPCS) == _declared_rpc_names()

    def test_lookup_keys_are_unique_so_no_handler_can_be_shadowed(self):
        # The server re-keys by lowercased name; a case-only collision would
        # silently drop a handler.
        assert len({rpc.lookup_key for rpc in RPCS.values()}) == len(RPCS)


class TestTransportOneofs:
    """Requests travel inside ControlRequest's oneof and replies inside
    ControlReturn's: a type never registered there cannot cross the wire,
    however correct the service definition and the handler are."""

    def test_every_request_type_is_a_control_request_oneof_member(self):
        members = _oneof_member_types(ControlRequest)
        # Positive membership: an empty derivation reports every RPC here
        # rather than passing vacuously.
        unroutable = sorted(
            name
            for name, rpc in RPCS.items()
            if rpc.handler.request_type not in members
        )
        assert unroutable == []

    def test_every_reply_type_is_a_control_return_oneof_member(self):
        members = _oneof_member_types(ControlReturn)
        unroutable = sorted(
            name for name, rpc in RPCS.items() if rpc.handler.reply_type not in members
        )
        assert unroutable == []


class TestHandlerCoverage:
    @pytest.mark.parametrize("rpc_name", SERVED_RPCS)
    def test_rpc_is_served_by_an_async_handler_class(self, rpc_name):
        defining_class = _defining_class(rpc_name)
        # WorkerServiceBase means the RPC still hits the UNIMPLEMENTED stub:
        # missing handler, or a method name that drifted from the proto.
        assert defining_class not in (None, WorkerServiceBase)
        assert issubclass(defining_class, ControlHandler)
        # The dispatcher awaits the result, so a plain def breaks at runtime.
        assert inspect.iscoroutinefunction(vars(defining_class)[rpc_name])

    def test_unimplemented_rpcs_are_exactly_the_acknowledged_set(self):
        # Both directions: implementing one of these and losing a handler
        # elsewhere both fail.
        unserved = {name for name in RPCS if _defining_class(name) is WorkerServiceBase}
        assert unserved == set(UNIMPLEMENTED_RPCS)

    def test_no_mixin_is_dead_weight(self):
        # A mixin whose method drifted from the proto name would sit in the
        # bases contributing nothing.
        served_by = {_defining_class(name) for name in SERVED_RPCS}
        assert [cls.__name__ for cls in _handler_mixins() if cls not in served_by] == []

    def test_every_handler_class_on_disk_is_wired_into_the_initializer(self):
        # Mirror of the MRO-based checks: a handler written but never added
        # to the bases is invisible to them and stays on the stub.
        on_disk = _handler_classes_on_disk()
        # Guard the scan itself: a walk gone (partly) blind fails here naming
        # the missed handlers, instead of blessing an empty scan.
        missed = [
            cls.__name__ for cls in _handler_mixins() if cls not in on_disk.values()
        ]
        assert missed == []
        wired = set(AsyncRPCHandlerInitializer.__mro__)
        unwired = sorted(name for name, cls in on_disk.items() if cls not in wired)
        assert unwired == []

    def test_no_rpc_is_defined_by_more_than_one_mixin(self):
        # MRO shadowing: the later of two definitions is dead code invisible
        # to every other check, wrong request type included.
        mixins = _handler_mixins()
        definers = {
            name: [cls.__name__ for cls in mixins if name in vars(cls)] for name in RPCS
        }
        assert {name: d for name, d in definers.items() if len(d) > 1} == {}

    def test_mixins_define_no_public_coroutine_outside_the_rpc_surface(self):
        # A public coroutine that is not a declared RPC is dead code, usually
        # a name that drifted from the proto. Private helpers are fine.
        stray = [
            f"{cls.__name__}.{name}"
            for cls in _handler_mixins()
            for name, value in vars(cls).items()
            if not name.startswith("_")
            and inspect.iscoroutinefunction(value)
            and name not in RPCS
        ]
        assert stray == []

    @pytest.mark.parametrize("rpc_name", SERVED_RPCS)
    def test_handler_signature_matches_the_generated_dispatch_shape(self, rpc_name):
        rpc = RPCS[rpc_name]
        function = vars(_defining_class(rpc_name))[rpc_name]
        # eval_str: compare types, not spellings, even under
        # `from __future__ import annotations`.
        signature = inspect.signature(function, eval_str=True)
        parameters = [p for p in signature.parameters.values() if p.name != "self"]
        # The generated wrapper calls `self.<rpc>(request)`: one positional
        # argument, awaited.
        assert len(parameters) == 1
        # Types come from the mapping, i.e. from the proto: a stale annotation
        # fails here instead of mis-parsing payloads at runtime.
        assert parameters[0].annotation is rpc.handler.request_type
        assert signature.return_annotation is rpc.handler.reply_type


class TestProductionLookup:
    """Covers the lookup shape AsyncRPCServer really builds and uses."""

    @pytest.fixture(scope="class")
    def server(self) -> AsyncRPCServer:
        # The one place in the suite where the production wiring is not
        # stubbed out.
        return AsyncRPCServer(InternalQueue(), object())

    def test_server_registers_one_key_per_declared_rpc(self, server):
        # receive() lowercases the incoming method_name before look_up.
        assert set(server._handlers) == {rpc.lookup_key for rpc in RPCS.values()}

    def test_every_registered_dispatcher_is_bound_to_the_real_initializer(self, server):
        # The stored dispatchers must be bound to the real initializer, not
        # to a stub exposing the same names.
        owners = {type(handler.func.__self__) for handler in server._handlers.values()}
        assert owners == {AsyncRPCHandlerInitializer}


def _invoke(
    server: AsyncRPCServer, wire_name: str, command_id: int
) -> ReturnInvocation:
    """Drive one ControlInvocation through the real server, return the reply.

    EmptyRequest for every RPC on purpose: the ones exercised here ignore the
    request or raise from the stub before reading it.
    """
    server.receive(
        ChannelIdentity(
            ActorVirtualIdentity("CONTROLLER"), ActorVirtualIdentity("worker-1"), True
        ),
        ControlInvocation(
            method_name=wire_name,
            command=set_one_of(ControlRequest, EmptyRequest()),
            command_id=command_id,
        ),
    )
    element = server._output_queue.get()
    # DirectControlMessagePayloadV2 uses the plain "value" group, not
    # "sealed_value".
    return get_one_of(element.payload, sealed=False)


class TestEndToEndThroughTheRealWiring:
    @pytest.fixture
    def server(self) -> AsyncRPCServer:
        # Function-scoped: these tests drain the output queue.
        return AsyncRPCServer(InternalQueue(), object())

    @pytest.mark.timeout(5)
    def test_a_real_rpc_round_trips_to_its_handler(self, server):
        # NoOperation touches neither context nor managers, so the full real
        # path can run unmodified.
        reply = _invoke(server, RPCS["no_operation"].wire_name, 1)
        assert reply.command_id == 1
        assert get_one_of(reply.return_value) == EmptyReturn()

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("rpc_name", sorted(UNIMPLEMENTED_RPCS))
    def test_an_unimplemented_rpc_replies_with_an_unimplemented_error(
        self, server, rpc_name
    ):
        # Still routable: the stub's GRPCError comes back as a ControlError
        # instead of killing the worker.
        reply = _invoke(server, RPCS[rpc_name].wire_name, 2)
        error = get_one_of(reply.return_value)
        assert isinstance(error, ControlError)
        # Derived, not spelled out: GRPCError's str() depends on enum repr,
        # which has churned across Python versions.
        assert error.error_message == str(
            grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)
        )
        assert error.language == ErrorLanguage.PYTHON
