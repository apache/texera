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

from threading import Condition, Event

from core.architecture.managers.tuple_processing_manager import TupleProcessingManager
from core.models import InternalMarker
from proto.org.apache.texera.amber.core import PortIdentity


class TestTupleProcessingManager:
    def test_initial_state(self):
        mgr = TupleProcessingManager()
        assert mgr.current_input_tuple is None
        assert mgr.current_input_port_id is None
        assert mgr.current_input_tuple_iter is None
        assert mgr.current_output_tuple is None
        assert mgr.current_internal_marker is None
        assert isinstance(mgr.context_switch_condition, Condition)
        assert isinstance(mgr.finished_current, Event)
        assert mgr.finished_current.is_set() is False

    def test_get_internal_marker_consume_once(self):
        mgr = TupleProcessingManager()
        marker = InternalMarker()
        mgr.current_internal_marker = marker
        assert mgr.get_internal_marker() is marker
        assert mgr.current_internal_marker is None
        assert mgr.get_internal_marker() is None

    def test_get_input_tuple_consume_once(self):
        mgr = TupleProcessingManager()
        sentinel = object()
        mgr.current_input_tuple = sentinel
        assert mgr.get_input_tuple() is sentinel
        assert mgr.current_input_tuple is None
        assert mgr.get_input_tuple() is None

    def test_get_output_tuple_consume_once(self):
        mgr = TupleProcessingManager()
        sentinel = object()
        mgr.current_output_tuple = sentinel
        assert mgr.get_output_tuple() is sentinel
        assert mgr.current_output_tuple is None
        assert mgr.get_output_tuple() is None

    def test_get_input_port_id_returns_zero_when_unset(self):
        # Documented "no upstream / source executor" fallback. Worth pinning
        # because it conflates "unset" with "real port id 0" — see the
        # follow-up test below that exposes the collision.
        mgr = TupleProcessingManager()
        assert mgr.current_input_port_id is None
        assert mgr.get_input_port_id() == 0

    def test_get_input_port_id_returns_real_port_id(self):
        mgr = TupleProcessingManager()
        mgr.current_input_port_id = PortIdentity(id=7, internal=False)
        assert mgr.get_input_port_id() == 7

    def test_get_input_port_id_collides_for_port_zero(self):
        # Pin: a real port with id=0 is indistinguishable from the
        # "no upstream" sentinel. If callers ever need to tell them apart,
        # the API has to change — this test guards the current behavior so
        # any future fix breaks it deliberately.
        mgr = TupleProcessingManager()
        mgr.current_input_port_id = PortIdentity(id=0, internal=False)
        assert mgr.get_input_port_id() == 0
        # And the sentinel path also returns 0.
        mgr.current_input_port_id = None
        assert mgr.get_input_port_id() == 0

    def test_finished_current_event_can_be_signalled(self):
        mgr = TupleProcessingManager()
        mgr.finished_current.set()
        assert mgr.finished_current.is_set() is True
        mgr.finished_current.clear()
        assert mgr.finished_current.is_set() is False

    def test_input_tuple_does_not_clear_output_or_marker(self):
        mgr = TupleProcessingManager()
        mgr.current_input_tuple = "in"
        mgr.current_output_tuple = "out"
        mgr.current_internal_marker = InternalMarker()
        mgr.get_input_tuple()
        assert mgr.current_output_tuple == "out"
        assert mgr.current_internal_marker is not None
