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

from core.architecture.managers.state_processing_manager import StateProcessingManager
from core.models.state import State


class TestStateProcessingManager:
    def test_initial_state_is_none(self):
        mgr = StateProcessingManager()
        assert mgr.current_input_state is None
        assert mgr.current_output_state is None
        assert mgr.get_input_state() is None
        assert mgr.get_output_state() is None

    def test_get_input_state_returns_then_clears(self):
        # The contract is "consume-once": the first get returns the stashed
        # value, and subsequent gets see None until the slot is set again.
        mgr = StateProcessingManager()
        s = State({"k": "v"})
        mgr.current_input_state = s
        assert mgr.get_input_state() is s
        assert mgr.current_input_state is None
        assert mgr.get_input_state() is None

    def test_get_output_state_returns_then_clears(self):
        mgr = StateProcessingManager()
        s = State({"k": "v"})
        mgr.current_output_state = s
        assert mgr.get_output_state() is s
        assert mgr.current_output_state is None
        assert mgr.get_output_state() is None

    def test_input_and_output_slots_are_independent(self):
        # Reading the input slot must not consume the output slot, and
        # vice versa — pin the no-cross-talk contract.
        mgr = StateProcessingManager()
        in_s = State({"side": "in"})
        out_s = State({"side": "out"})
        mgr.current_input_state = in_s
        mgr.current_output_state = out_s

        assert mgr.get_input_state() is in_s
        assert mgr.current_output_state is out_s
        assert mgr.get_output_state() is out_s
