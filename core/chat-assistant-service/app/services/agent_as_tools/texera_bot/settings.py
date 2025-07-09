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

from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    planner_model: str = "gpt-4.1"
    builder_model: str = "gpt-4o-mini"
    manager_model: str = "gpt-4o-mini"
    temperature: float = 0.1
    top_p: float = 1.0
    viz_dir: str = "viz"
