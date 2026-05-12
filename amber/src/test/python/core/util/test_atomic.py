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

import threading

import pytest

from core.util.atomic import AtomicInteger


class TestAtomicIntegerSingleThreaded:
    def test_default_starts_at_zero(self):
        assert AtomicInteger().value == 0

    def test_initial_value_is_coerced_to_int(self):
        # The constructor wraps the input through int(), which lets callers
        # pass a numeric string or float and still get a clean integer state.
        assert AtomicInteger("7").value == 7
        assert AtomicInteger(3.9).value == 3  # int() truncates toward zero

    def test_inc_returns_new_value_after_adding_default_one(self):
        a = AtomicInteger(10)
        assert a.inc() == 11
        assert a.value == 11

    def test_inc_with_custom_delta_uses_int_coercion(self):
        a = AtomicInteger(10)
        assert a.inc(5) == 15
        # int("3") -> 3, the underlying state increments by 3.
        assert a.inc("3") == 18

    def test_dec_is_inc_with_negated_delta(self):
        a = AtomicInteger(10)
        assert a.dec() == 9
        assert a.dec(4) == 5

    def test_get_and_inc_returns_pre_increment_value(self):
        a = AtomicInteger(10)
        assert a.get_and_inc() == 10
        assert a.value == 11

    def test_get_and_dec_returns_pre_decrement_value(self):
        a = AtomicInteger(10)
        assert a.get_and_dec(2) == 10
        assert a.value == 8

    def test_value_setter_replaces_state_with_int_coercion(self):
        a = AtomicInteger(10)
        a.value = 42
        assert a.value == 42
        a.value = "100"
        assert a.value == 100

    def test_get_and_set_currently_deadlocks_on_non_reentrant_lock(self):
        # Bug pin: get_and_set acquires self._lock and then reads self.value,
        # which is a property that ALSO tries to acquire self._lock. The lock
        # is a non-reentrant threading.Lock, so the call deadlocks the moment
        # it is invoked. Document via thread + timeout so the test surfaces
        # the deadlock without hanging the whole suite, and pair it with an
        # xfail-strict test below that asserts the intended contract.
        a = AtomicInteger(10)
        started = threading.Event()
        completed = threading.Event()
        errors: list[BaseException] = []

        def attempt():
            started.set()
            try:
                a.get_and_set(99)
                completed.set()
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=attempt, daemon=True)
        worker.start()
        # Make sure the worker actually entered `attempt` — otherwise a
        # scheduling delay alone could let the assertions below pass even on
        # a fixed implementation.
        assert started.wait(timeout=2.0), "worker thread never started"
        # Give get_and_set a moment to either deadlock or return.
        completed.wait(timeout=0.5)
        assert not errors, (
            f"get_and_set raised before reaching the deadlock spin: {errors[0]!r}"
        )
        assert worker.is_alive(), (
            "worker thread exited unexpectedly — get_and_set neither deadlocked "
            "nor completed; the test no longer pins the documented bug."
        )
        assert not completed.is_set(), (
            "get_and_set unexpectedly returned — the deadlock bug appears fixed; "
            "delete this pinned test along with the xfail below."
        )

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known bug: AtomicInteger.get_and_set deadlocks because it holds "
            "the non-reentrant lock while accessing the value property. "
            "This xfail flips to XPASS when the bug is fixed."
        ),
    )
    @pytest.mark.timeout(2)
    def test_get_and_set_should_return_old_value_and_replace_state(self):
        a = AtomicInteger(10)
        assert a.get_and_set(99) == 10
        assert a.value == 99


class TestAtomicIntegerThreadSafety:
    def test_inc_under_concurrent_threads_is_lossless(self):
        a = AtomicInteger(0)
        threads_count = 8
        per_thread = 1000

        def worker():
            for _ in range(per_thread):
                a.inc()

        threads = [threading.Thread(target=worker) for _ in range(threads_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert a.value == threads_count * per_thread
