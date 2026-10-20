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

from unittest.mock import MagicMock

from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)


class TestActorCommandHandler:
    def test_the_base_handler_is_an_inert_no_op(self):
        # ActorCommandHandler declares no abstract members, so an unfinished
        # subclass -- or the base itself -- is instantiable and callable. The
        # base __call__ must then do nothing at all: return None and touch
        # neither of its two arguments, rather than half-handling the
        # command. Both arguments are bound to names so a side effect on
        # either is visible -- passing the command inline as an anonymous
        # MagicMock hides every mutation that only writes to it.
        handler = ActorCommandHandler()
        command = MagicMock()
        input_queue = MagicMock()

        result = handler(command, input_queue)

        assert result is None
        input_queue.assert_not_called()
        assert input_queue.method_calls == []
        assert command.mock_calls == []
        # Subclasses opt in by setting `cmd`; the base must not claim one, or
        # the handler registry would dispatch every command to the no-op.
        assert ActorCommandHandler.cmd is None
