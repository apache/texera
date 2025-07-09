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

import asyncio
import logging
import uuid
from typing import Dict, List, Any

from agents import function_tool
from starlette.websockets import WebSocket

from app.models.types import OperatorAndPosition, OperatorLink

logger = logging.getLogger(__name__)


class ToolRegistry:
    """Encapsulate websocket‑backed Texera tools."""

    def __init__(
        self,
        parent_service,  # NEW
        websocket: WebSocket,
        schema_futs: Dict[str, asyncio.Future],
        add_op_futs: Dict[str, asyncio.Future],
        current_dag: List[Any],
    ):
        self.service = parent_service  # NEW
        self.websocket = websocket
        self._schema_futs = schema_futs
        self._add_op_futs = add_op_futs
        self._current_dag = current_dag

    def _create_get_operator_schema(self):
        @function_tool(strict_mode=False)
        async def get_operator_schema(operator_type: str) -> Dict[str, Any]:
            request_id = str(uuid.uuid4())
            fut = asyncio.get_event_loop().create_future()
            self._schema_futs[request_id] = fut
            if self.service.session_id:
                self.service.rid_registry[request_id] = self.service.session_id

            await self.websocket.send_json(
                {
                    "type": "getOperatorSchema",
                    "operatorType": operator_type,
                    "requestId": request_id,
                    "sessionId": self.service.session_id,
                }
            )
            result = await fut
            del self._schema_futs[request_id]
            return result

        return get_operator_schema

    def _create_add_operator_and_links(self):
        @function_tool(strict_mode=False)
        async def add_operator_and_links(
            operator_and_position: OperatorAndPosition, links: List[OperatorLink]
        ) -> str:
            request_id = str(uuid.uuid4())
            fut = asyncio.get_event_loop().create_future()
            self._add_op_futs[request_id] = fut
            op_payload = operator_and_position.dict()
            links_payload = [link.dict() for link in links]
            if self.service.session_id:
                self.service.rid_registry[request_id] = self.service.session_id

            await self.websocket.send_json(
                {
                    "type": "addOperatorAndLinks",
                    "operatorAndPosition": op_payload,
                    "links": links_payload,
                    "requestId": request_id,
                    "sessionId": self.service.session_id,
                }
            )
            status = await fut
            del self._add_op_futs[request_id]
            if status == "success":
                self._current_dag.append(
                    {"operator_and_position": op_payload, "links": links}
                )
            return status

        return add_operator_and_links

    def _create_get_current_dag(self):
        @function_tool(strict_mode=False)
        def get_current_dag() -> List[Any]:
            return self._current_dag

        return get_current_dag

    def build(self):
        return (
            self._create_get_operator_schema(),
            self._create_add_operator_and_links(),
            self._create_get_current_dag(),
        )
