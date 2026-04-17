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

from typing import Optional, List

from core.models import ExceptionInfo


class ExceptionManager:
    def __init__(self):
        self.exc_info: Optional[ExceptionInfo] = None
        self.exc_info_history: List[ExceptionInfo] = list()

    def set_exception_info(self, exc_info: ExceptionInfo) -> None:
        self.exc_info = exc_info
        self.exc_info_history.append(exc_info)

    def has_exception(self) -> bool:
        return self.exc_info is not None

    def get_exc_info(self) -> ExceptionInfo:
        exc_info = self.exc_info
        self.exc_info = None
        return exc_info
