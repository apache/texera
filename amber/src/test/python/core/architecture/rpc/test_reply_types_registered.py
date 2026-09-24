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

import dataclasses
import inspect
import typing

import proto.org.apache.texera.amber.engine.architecture.rpc as rpc
from core.util import get_one_of, set_one_of
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlReturn,
    CoordinatorServiceStub,
    WorkerServiceStub,
)


def _registered_member_types() -> dict:
    """The message types registered in ControlReturn's sealed oneof, by field."""
    hints = typing.get_type_hints(ControlReturn)
    return {
        field.name: hints[field.name]
        for field in dataclasses.fields(ControlReturn)
        if getattr(field.metadata["betterproto"], "group", None) == "sealed_value"
    }


def _declared_reply_types(stub_class) -> dict:
    """The declared reply type of every RPC method on a generated service stub."""
    reply_types = {}
    for name, method in vars(stub_class).items():
        if not inspect.iscoroutinefunction(method):
            continue
        annotation = method.__annotations__["return"]
        reply_types[name] = (
            getattr(rpc, annotation) if isinstance(annotation, str) else annotation
        )
    return reply_types


class TestControlReturnRegistry:
    def test_every_declared_rpc_reply_type_is_a_registered_oneof_member(self):
        # Every reply travels inside ControlReturn's sealed oneof, and
        # set_one_of silently packs an unregistered type into an empty
        # ControlReturn (see test_async_rpc_server.py, which pins that
        # mechanism). A reply type declared by an RPC but missing from the
        # oneof is therefore dropped on the wire without any error, so every
        # declared reply type must be registered in controlreturns.proto.
        registered = set(_registered_member_types().values())
        assert registered, (
            "Reflection found no sealed_value members on ControlReturn; the "
            "generated-code layout changed and this test no longer checks "
            "anything."
        )
        for stub_class in (WorkerServiceStub, CoordinatorServiceStub):
            declared = _declared_reply_types(stub_class)
            assert declared, (
                f"Reflection found no RPC methods on {stub_class.__name__}; "
                f"the generated-stub layout changed and this test no longer "
                f"checks anything."
            )
            unregistered = {
                f"{stub_class.__name__}.{method} -> {reply.__name__}"
                for method, reply in declared.items()
                if reply not in registered
            }
            assert not unregistered, (
                f"RPC reply types not registered in ControlReturn's sealed "
                f"oneof: {sorted(unregistered)}. Register each type in "
                f"controlreturns.proto, otherwise its replies are silently "
                f"dropped."
            )

    def test_every_registered_member_survives_set_one_of(self):
        # set_one_of derives the oneof field name from the type name, so a
        # member whose field name does not follow that convention is packed
        # into nothing. Round-tripping every member through set_one_of keeps
        # the field names and the conversion logic from drifting apart.
        members = _registered_member_types()
        assert members, (
            "Reflection found no sealed_value members on ControlReturn; the "
            "generated-code layout changed and this test no longer checks "
            "anything."
        )
        for field_name, member_type in members.items():
            member = member_type()
            packed = set_one_of(ControlReturn, member)
            assert get_one_of(packed) is member, (
                f"set_one_of failed to pack {member_type.__name__} into "
                f"ControlReturn.{field_name}; the oneof field name does not "
                f"match the name set_one_of derives from the type."
            )
