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

import os
from typing import Callable

from agents import Agent, OpenAIResponsesModel, ModelSettings
from openai import AsyncOpenAI

from app.services.agent_as_tools.texera_bot.prompt_loader import load_prompt
from app.services.agent_as_tools.texera_bot.util_tools import gen_uuid
from app.services.agent_as_tools.texera_bot.settings import Settings


PLANNER_SYS = load_prompt("planner_sys")
BUILDER_SYS = load_prompt("builder_sys")
MANAGER_SYS = load_prompt("manager_sys")


class AgentFactory:
    """
    Build planner, builder, and manager agents using a ToolRegistry.
    Also draws their graphs once.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        openai_client: AsyncOpenAI,
        graph_drawer: Callable,
        get_schema,
        add_ops,
        get_current_dag,
    ):
        self.settings = settings
        self.openai_client = openai_client
        self.graph_drawer = graph_drawer
        self.get_schema = get_schema
        self.add_ops = add_ops
        self.get_current_dag = get_current_dag

    def build(self) -> Agent:
        # ── planner ─────────────────────────────
        planner = Agent(
            name="planner_agent",
            instructions=PLANNER_SYS,
            model=OpenAIResponsesModel(
                model=self.settings.planner_model, openai_client=self.openai_client
            ),
            tools=[self.get_current_dag],
            model_settings=ModelSettings(
                temperature=self.settings.temperature, top_p=self.settings.top_p
            ),
        )

        # ── builder ─────────────────────────────
        builder = Agent(
            name="builder_agent",
            instructions=BUILDER_SYS,
            model=OpenAIResponsesModel(
                model=self.settings.builder_model, openai_client=self.openai_client
            ),
            tools=[self.get_current_dag, gen_uuid, self.get_schema],
            model_settings=ModelSettings(
                temperature=self.settings.temperature, top_p=self.settings.top_p
            ),
        )

        # ── manager ─────────────────────────────
        manager = Agent(
            name="texera_bot_manager_agent",
            instructions=MANAGER_SYS,
            model=OpenAIResponsesModel(
                model=self.settings.manager_model, openai_client=self.openai_client
            ),
            tools=[
                planner.as_tool(
                    tool_name="planner_agent",
                    tool_description=(
                        "Design Texera workflows at a high level. "
                        "Argument must be the user's prompt string."
                    ),
                ),
                builder.as_tool(
                    tool_name="builder_agent",
                    tool_description=(
                        "Instantiate a Texera operator. Argument must be a JSON "
                        "string with keys `operatorToBuild` and `overallPlan`."
                    ),
                ),
                self.add_ops,
                self.get_current_dag
            ],
            model_settings=ModelSettings(
                temperature=self.settings.temperature, top_p=self.settings.top_p
            ),
        )

        return manager
