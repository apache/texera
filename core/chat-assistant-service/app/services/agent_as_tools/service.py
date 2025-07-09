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
from typing import Any, Dict, List, AsyncGenerator  # add Callable

from agents import Agent, Runner, OpenAIResponsesModel, Model, AsyncOpenAI
from agents.extensions.visualization import draw_graph
from openai.types.responses import ResponseTextDeltaEvent, ResponseCompletedEvent
from starlette.websockets import WebSocket

from app.services.agent_as_tools.texera_bot.agent_factory import AgentFactory
from app.services.agent_as_tools.texera_bot.settings import Settings
from app.services.agent_as_tools.texera_bot.tool_registry import ToolRegistry

settings = Settings()

logger = logging.getLogger(__name__)


class AgentService:
    def __init__(
        self,
        model: str | Model = OpenAIResponsesModel(
            model="gpt-4o-mini", openai_client=AsyncOpenAI()
        ),
        session_id: str | None = None,
        rid_registry: Dict[str, str] | None = None,
        trace_cm=None,
    ):
        self.model = model
        self.session_id = session_id
        self.rid_registry = rid_registry
        self._trace_cm = trace_cm
        self._schema_futures: Dict[str, asyncio.Future] = {}
        self._add_op_futures: Dict[str, asyncio.Future] = {}
        self._context: List[Dict[str, Any]] = []
        self._current_dag: List[Any] = []

    def close(self):
        if self._trace_cm:
            self._trace_cm.__exit__(None, None, None)

    @property
    def context(self) -> List[Dict[str, Any]]:
        return self._context

    # ─────────────────────────────────────────────────────────────
    # NEW – construct the sub-agents and the manager
    # ─────────────────────────────────────────────────────────────
    def _make_agents(self, websocket: WebSocket) -> Agent:
        registry = ToolRegistry(
            self,  # NEW  (parent_service)
            websocket,
            self._schema_futures,
            self._add_op_futures,
            self._current_dag,
        )
        get_schema, add_ops, get_current_dag = registry.build()

        factory = AgentFactory(
            settings=settings,
            openai_client=AsyncOpenAI(),
            graph_drawer=draw_graph,
            get_schema=get_schema,
            add_ops=add_ops,
            get_current_dag=get_current_dag,
        )
        return factory.build()

    # ─────────────────────────────────────────────────────────────
    # Streaming chat entry-point
    # ─────────────────────────────────────────────────────────────
    async def stream_chat(
        self, websocket: WebSocket, user_message: str
    ) -> AsyncGenerator[str, None]:
        manager = self._make_agents(websocket)
        conversation = self._context + [{"role": "user", "content": user_message}]

        result = Runner.run_streamed(
            manager,
            input=conversation,
            max_turns=100,
        )

        async for evt in result.stream_events():
            if evt.type == "raw_response_event":
                if isinstance(evt.data, ResponseTextDeltaEvent):
                    yield evt.data.delta
                elif isinstance(evt.data, ResponseCompletedEvent):
                    # update shared context and exit
                    self._context = result.to_input_list()
                else:
                    pass
        if result.is_complete and result.final_output:
            self._context = result.to_input_list()
