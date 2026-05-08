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

from collections import defaultdict
from typing import DefaultDict

from loguru import logger

from proto.org.apache.texera.amber.core import PortIdentity
from proto.org.apache.texera.amber.engine.architecture.worker import (
    WorkerStatistics,
    PortTupleMetricsMapping,
    TupleMetrics,
)


class StatisticsManager:
    def __init__(self) -> None:
        # Initialize metrics with default values
        self._input_tuple_metrics: DefaultDict[PortIdentity, TupleMetrics] = (
            defaultdict(lambda: (0, 0))
        )
        self._output_tuple_metrics: DefaultDict[PortIdentity, TupleMetrics] = (
            defaultdict(lambda: (0, 0))
        )
        self._data_processing_time: int = 0
        self._control_processing_time: int = 0
        self._total_execution_time: int = 0
        self._worker_start_time: int = 0

    def get_statistics(self) -> WorkerStatistics:
        # Compile and return worker statistics
        return WorkerStatistics(
            [
                PortTupleMetricsMapping(port_id, TupleMetrics(*tuple_metrics))
                for port_id, tuple_metrics in self._input_tuple_metrics.items()
            ],
            [
                PortTupleMetricsMapping(port_id, TupleMetrics(*tuple_metrics))
                for port_id, tuple_metrics in self._output_tuple_metrics.items()
            ],
            self._data_processing_time,
            self._control_processing_time,
            max(
                0,
                self._total_execution_time
                - self._data_processing_time
                - self._control_processing_time,
            ),
        )

    def increase_input_statistics(self, port_id: PortIdentity, size: int) -> None:
        if size < 0:
            raise ValueError("Tuple size must be non-negative")
        count, total_size = self._input_tuple_metrics[port_id]
        self._input_tuple_metrics[port_id] = (count + 1, total_size + size)

    def increase_output_statistics(self, port_id: PortIdentity, size: int) -> None:
        if size < 0:
            raise ValueError("Tuple size must be non-negative")
        count, total_size = self._output_tuple_metrics[port_id]
        self._output_tuple_metrics[port_id] = (count + 1, total_size + size)

    def increase_data_processing_time(self, time: int) -> None:
        if time < 0:
            raise ValueError("Time must be non-negative")
        self._data_processing_time += time

    def increase_control_processing_time(self, time: int) -> None:
        if time < 0:
            raise ValueError("Time must be non-negative")
        self._control_processing_time += time

    def update_total_execution_time(self, time: int) -> None:
        if time < self._worker_start_time:
            raise ValueError(
                "Current time must be greater than or equal to worker start time"
            )
        new_total = time - self._worker_start_time
        if new_total < self._total_execution_time:
            logger.warning(
                f"update_total_execution_time called with non-monotonic time: "
                f"new total {new_total}ns < current total {self._total_execution_time}ns. "
                "Clock skew or out-of-order call detected."
            )
        processing_total = self._data_processing_time + self._control_processing_time
        if new_total < processing_total:
            logger.warning(
                f"idle_time drift: total_execution_time ({new_total}ns) < "
                f"data ({self._data_processing_time}ns) + control "
                f"({self._control_processing_time}ns). "
                "update_total_execution_time should be called after increase_*_processing_time "
                "with the same end timestamp. idle_time will be clamped to 0."
            )
        self._total_execution_time = new_total

    def initialize_worker_start_time(self, time: int) -> None:
        # Set the worker start time
        self._worker_start_time = time
