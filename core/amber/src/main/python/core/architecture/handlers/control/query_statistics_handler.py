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

from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    WorkerMetricsResponse,
    EmptyRequest,
)
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerMetrics,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler


class QueryStatisticsHandler(ControlHandler):

    async def query_statistics(self, req: EmptyRequest) -> WorkerMetricsResponse:
        metrics = WorkerMetrics(
            worker_state=self.context.state_manager.get_current_state(),
            worker_statistics=self.context.statistics_manager.get_statistics(),
        )
        return WorkerMetricsResponse(metrics)
