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
from loguru import logger

from core.architecture.managers.statistics_manager import StatisticsManager
from proto.org.apache.texera.amber.core import PortIdentity


def _port(pid: int) -> PortIdentity:
    return PortIdentity(id=pid, internal=False)


class TestStatisticsManagerDefaults:
    def test_get_statistics_with_no_activity(self):
        stats = StatisticsManager().get_statistics()
        assert list(stats.input_tuple_metrics) == []
        assert list(stats.output_tuple_metrics) == []
        assert stats.data_processing_time == 0
        assert stats.control_processing_time == 0
        # idle_time = total_execution - data - control = 0 at init.
        assert stats.idle_time == 0


class TestStatisticsManagerInputOutput:
    def test_increase_input_aggregates_count_and_size_per_port(self):
        mgr = StatisticsManager()
        mgr.increase_input_statistics(_port(0), 10)
        mgr.increase_input_statistics(_port(0), 5)
        mgr.increase_input_statistics(_port(1), 7)

        stats = mgr.get_statistics()
        by_port = {m.port_id.id: m.tuple_metrics for m in stats.input_tuple_metrics}
        assert by_port[0].count == 2
        assert by_port[0].size == 15
        assert by_port[1].count == 1
        assert by_port[1].size == 7
        # Output side stayed empty.
        assert list(stats.output_tuple_metrics) == []

    def test_increase_output_aggregates_count_and_size_per_port(self):
        mgr = StatisticsManager()
        mgr.increase_output_statistics(_port(2), 100)
        mgr.increase_output_statistics(_port(2), 200)

        stats = mgr.get_statistics()
        by_port = {m.port_id.id: m.tuple_metrics for m in stats.output_tuple_metrics}
        assert by_port[2].count == 2
        assert by_port[2].size == 300
        assert list(stats.input_tuple_metrics) == []

    def test_zero_size_input_is_allowed(self):
        # Pin: zero is valid (size validation is `< 0`, not `<= 0`).
        # Empty tuples / heartbeat-style records can legitimately be size 0.
        mgr = StatisticsManager()
        mgr.increase_input_statistics(_port(0), 0)
        stats = mgr.get_statistics()
        m = list(stats.input_tuple_metrics)[0].tuple_metrics
        assert m.count == 1
        assert m.size == 0

    @pytest.mark.parametrize(
        "method", ["increase_input_statistics", "increase_output_statistics"]
    )
    def test_negative_size_raises(self, method):
        mgr = StatisticsManager()
        with pytest.raises(ValueError, match="Tuple size must be non-negative"):
            getattr(mgr, method)(_port(0), -1)


class TestStatisticsManagerProcessingTime:
    def test_data_and_control_time_accumulate(self):
        mgr = StatisticsManager()
        mgr.increase_data_processing_time(100)
        mgr.increase_data_processing_time(50)
        mgr.increase_control_processing_time(20)
        stats = mgr.get_statistics()
        assert stats.data_processing_time == 150
        assert stats.control_processing_time == 20

    def test_zero_processing_time_is_allowed(self):
        mgr = StatisticsManager()
        mgr.increase_data_processing_time(0)
        mgr.increase_control_processing_time(0)
        stats = mgr.get_statistics()
        assert stats.data_processing_time == 0
        assert stats.control_processing_time == 0

    @pytest.mark.parametrize(
        "method",
        ["increase_data_processing_time", "increase_control_processing_time"],
    )
    def test_negative_time_raises(self, method):
        mgr = StatisticsManager()
        with pytest.raises(ValueError, match="Time must be non-negative"):
            getattr(mgr, method)(-1)


class TestStatisticsManagerExecutionTime:
    def test_total_execution_time_is_relative_to_worker_start(self):
        mgr = StatisticsManager()
        mgr.initialize_worker_start_time(1_000)
        mgr.update_total_execution_time(1_500)
        stats = mgr.get_statistics()
        # idle = total_exec - data - control = 500 - 0 - 0
        assert stats.idle_time == 500

    def test_total_execution_time_equal_to_start_is_allowed(self):
        # The validation is `time < start`, so equality is OK and yields 0.
        mgr = StatisticsManager()
        mgr.initialize_worker_start_time(1_000)
        mgr.update_total_execution_time(1_000)
        assert mgr.get_statistics().idle_time == 0

    def test_total_execution_time_before_start_raises(self):
        mgr = StatisticsManager()
        mgr.initialize_worker_start_time(1_000)
        with pytest.raises(
            ValueError,
            match="Current time must be greater than or equal to worker start time",
        ):
            mgr.update_total_execution_time(999)

    def test_idle_time_clamped_to_zero_when_processing_overshoots(self):
        # When data+control exceed total_execution_time (e.g. update_total was
        # called before all increase_* calls for that interval), idle_time is
        # clamped to 0. It must never be negative.
        # No drift warning fires HERE, despite the shape of the scenario: the
        # increase_* calls come after update_total, so processing_total was
        # still 0 when the guard ran. The warning path needs the opposite
        # order and is covered in TestStatisticsManagerDriftWarnings.
        mgr = StatisticsManager()
        mgr.initialize_worker_start_time(1_000)
        mgr.update_total_execution_time(1_100)  # 100ns total
        mgr.increase_data_processing_time(80)
        mgr.increase_control_processing_time(50)  # 130 > 100
        assert mgr.get_statistics().idle_time == 0


def _capture(call) -> list:
    """Run `call` with a record-capturing loguru sink attached and return the
    records. The sink is attached at DEBUG, not WARNING, so that a diagnostic
    silently DEMOTED below WARNING stays distinguishable from one that is gone
    -- a WARNING-level sink cannot tell those two apart. loguru's logger is a
    process-global singleton and the pytest process is shared, so the removal
    has to happen in a finally or a leaked sink poisons every sibling suite.
    (loguru does not propagate to stdlib logging, so caplog is not an option.)
    """
    records: list = []
    handler_id = logger.add(lambda m: records.append(m.record), level="DEBUG")
    try:
        call()
    finally:
        logger.remove(handler_id)
    return records


def _messages(call) -> list:
    return [r["message"] for r in _capture(call)]


class TestStatisticsManagerDriftWarnings:
    """The two warning paths in update_total_execution_time. Both are pure
    diagnostics -- the value is still stored -- so the assertions pin the
    message CONTENT and the log LEVEL, otherwise swapping the two warning
    bodies, or escalating one to ERROR, would survive."""

    def test_non_monotonic_total_execution_time_warns_and_still_stores(self):
        mgr = StatisticsManager()
        # The worker start time and the stored total are deliberately DIFFERENT
        # literals (100 vs 1000). Were they equal, the message assertion below
        # could be satisfied by _worker_start_time standing in for
        # _total_execution_time, and would then pin neither field.
        mgr.initialize_worker_start_time(100)
        mgr.update_total_execution_time(1_100)  # total_execution_time = 1000

        # new_total = 500 < stored 1000 -> clock went backwards.
        records = _capture(lambda: mgr.update_total_execution_time(600))

        joined = "".join(r["message"] for r in records)
        assert "non-monotonic time" in joined
        assert "new total 500ns < current total 1000ns" in joined
        # Not the other warning: 500 >= data(0) + control(0).
        assert "idle_time drift" not in joined
        # A defensive diagnostic against clock skew, not an alert: exactly one
        # record, and it stays at WARNING.
        assert [r["level"].name for r in records] == ["WARNING"]
        # Last write still wins -- the warning does not veto the update.
        assert mgr.get_statistics().idle_time == 500

        # Boundary: the guard is `<`, so re-sending the SAME timestamp is
        # monotonic and must stay silent. main_loop calls this repeatedly, so
        # `<=` here would warn on every unchanged timestamp.
        assert _messages(lambda: mgr.update_total_execution_time(600)) == []
        assert mgr.get_statistics().idle_time == 500

    def test_idle_drift_warns_naming_data_and_control_totals(self):
        mgr = StatisticsManager()
        mgr.initialize_worker_start_time(1_000)
        mgr.increase_data_processing_time(80)
        mgr.increase_control_processing_time(50)  # processing_total = 130

        # new_total = 100 < 130 -> idle_time would go negative.
        records = _capture(lambda: mgr.update_total_execution_time(1_100))

        joined = "".join(r["message"] for r in records)
        assert "idle_time drift" in joined
        assert "total_execution_time (100ns) < data (80ns) + control (50ns)" in joined
        # Not the other warning: 100 >= stored total 0.
        assert "non-monotonic time" not in joined
        assert [r["level"].name for r in records] == ["WARNING"]
        assert mgr.get_statistics().idle_time == 0

        # No false alarm on the boundary. A fresh manager whose total lands
        # EXACTLY on data+control has idle_time 0, not negative, so neither
        # warning may fire. This pins both operands of the comparison as well
        # as the boundary: comparing the stored total instead of new_total, or
        # multiplying data by control instead of adding them, each makes this
        # block warn.
        mgr2 = StatisticsManager()
        mgr2.initialize_worker_start_time(1_000)
        mgr2.increase_data_processing_time(80)
        mgr2.increase_control_processing_time(50)
        assert _messages(lambda: mgr2.update_total_execution_time(1_130)) == []
        assert mgr2.get_statistics().idle_time == 0
