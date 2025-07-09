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
from typing import Any, Dict, List, AsyncGenerator

from agents import Agent, Runner, function_tool
from openai.types.responses import ResponseTextDeltaEvent, ResponseCompletedEvent
from starlette.websockets import WebSocket

from app.services.single_agent.prompt import SYS_INSTRUCTIONS
from app.models.types import OperatorAndPosition, OperatorLink

logger = logging.getLogger(__name__)


class AgentService:
    def __init__(self, model: str = "gpt-4o-mini"):
        self.model = model
        self._schema_futures: Dict[str, asyncio.Future] = {}
        self._add_op_futures: Dict[str, asyncio.Future] = {}
        self._context: List[Dict[str, Any]] = []

    @property
    def context(self) -> List[Dict[str, Any]]:
        return self._context

    def _register_tools(self, websocket: WebSocket):
        @function_tool(strict_mode=False)
        async def get_operator_schema(operator_type: str) -> Dict[str, Any]:
            request_id = str(uuid.uuid4())
            fut = asyncio.get_event_loop().create_future()
            self._schema_futures[request_id] = fut
            await websocket.send_json(
                {
                    "type": "getOperatorSchema",
                    "operatorType": operator_type,
                    "requestId": request_id,
                }
            )
            result = await fut
            del self._schema_futures[request_id]
            return result

        @function_tool(strict_mode=False)
        async def add_operator_and_links(
            operator_and_position: OperatorAndPosition, links: List[OperatorLink]
        ) -> str:
            request_id = str(uuid.uuid4())
            fut = asyncio.get_event_loop().create_future()
            self._add_op_futures[request_id] = fut
            op_payload = operator_and_position.dict()
            links_payload = [link.dict() for link in links]
            await websocket.send_json(
                {
                    "type": "addOperatorAndLinks",
                    "operatorAndPosition": op_payload,
                    "links": links_payload,
                    "requestId": request_id,
                }
            )
            status = await fut
            del self._add_op_futures[request_id]
            return status

        return get_operator_schema, add_operator_and_links

    async def stream_chat(
        self, websocket: WebSocket, user_message: str
    ) -> AsyncGenerator[str, None]:
        get_schema, add_ops = self._register_tools(websocket)
        conversation = self._context + [{"role": "user", "content": user_message}]
        agent = Agent(
            name="Texera Workflow Builder",
            instructions=SYS_INSTRUCTIONS,
            model=self.model,
            tools=[get_schema, add_ops],
        )
        result = Runner.run_streamed(agent, input=conversation, max_turns=100)

        async for evt in result.stream_events():
            if evt.type == "raw_response_event":
                if isinstance(evt.data, ResponseTextDeltaEvent):
                    yield evt.data.delta
                elif isinstance(evt.data, ResponseCompletedEvent):
                    # update shared context and exit
                    self._context = result.to_input_list()
                else:
                    pass
                    # logger.warning(f"Unexpected response event: {evt.data}")
            # else:
            #     logger.warning(f"Unexpected event: {evt}")

        # safety net: if we exit without a completion event
        if result.is_complete and result.final_output:
            self._context = result.to_input_list()
