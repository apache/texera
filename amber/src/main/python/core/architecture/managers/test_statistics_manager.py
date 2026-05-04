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


class TestStatisticsManagerIdleTime:
    def test_idle_time_accumulates(self):
        mgr = StatisticsManager()
        mgr.increase_idle_time(100)
        mgr.increase_idle_time(50)
        assert mgr.get_statistics().idle_time == 150

    def test_idle_time_zero_by_default(self):
        assert StatisticsManager().get_statistics().idle_time == 0

    def test_zero_idle_time_is_allowed(self):
        mgr = StatisticsManager()
        mgr.increase_idle_time(0)
        assert mgr.get_statistics().idle_time == 0

    def test_negative_idle_time_raises(self):
        mgr = StatisticsManager()
        with pytest.raises(ValueError, match="Time must be non-negative"):
            mgr.increase_idle_time(-1)

    def test_idle_time_independent_of_processing_times(self):
        mgr = StatisticsManager()
        mgr.increase_idle_time(200)
        mgr.increase_data_processing_time(300)
        mgr.increase_control_processing_time(100)
        stats = mgr.get_statistics()
        assert stats.idle_time == 200
        assert stats.data_processing_time == 300
        assert stats.control_processing_time == 100
