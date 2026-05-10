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

import sys

from core.architecture.managers.exception_manager import ExceptionManager
from core.models import ExceptionInfo


def _real_exc_info() -> ExceptionInfo:
    """Build a real ExceptionInfo by raising and catching, so the traceback
    object is the actual one Python produces."""
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        exc, value, tb = sys.exc_info()
        return ExceptionInfo(exc=exc, value=value, tb=tb)


class TestExceptionManager:
    def test_initial_state(self):
        mgr = ExceptionManager()
        assert mgr.exc_info is None
        assert mgr.exc_info_history == []
        assert mgr.has_exception() is False

    def test_set_then_has_exception_true(self):
        mgr = ExceptionManager()
        info = _real_exc_info()
        mgr.set_exception_info(info)
        assert mgr.has_exception() is True
        assert mgr.exc_info is info
        assert mgr.exc_info_history == [info]

    def test_get_exc_info_returns_and_clears_current_only(self):
        # Pin the documented contract: get_exc_info returns the latest stashed
        # info AND clears the live slot, but the history must keep it. A
        # regression that also clears history would break replay/retry flows.
        mgr = ExceptionManager()
        info = _real_exc_info()
        mgr.set_exception_info(info)

        assert mgr.get_exc_info() is info
        assert mgr.exc_info is None
        assert mgr.has_exception() is False
        assert mgr.exc_info_history == [info]

    def test_get_exc_info_when_none_returns_none(self):
        mgr = ExceptionManager()
        assert mgr.get_exc_info() is None

    def test_history_accumulates_in_order(self):
        mgr = ExceptionManager()
        first = _real_exc_info()
        second = _real_exc_info()
        mgr.set_exception_info(first)
        mgr.set_exception_info(second)
        assert mgr.exc_info is second
        assert mgr.exc_info_history == [first, second]
        # Consuming the latest must leave both entries in history.
        assert mgr.get_exc_info() is second
        assert mgr.exc_info_history == [first, second]
