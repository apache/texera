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

from threading import Event, Condition
from typing import Optional, Tuple, Iterator

from core.models import InternalMarker
from proto.edu.uci.ics.amber.core import PortIdentity


class TupleProcessingManager:
    def __init__(self):
        self.current_input_tuple: Optional[Tuple] = None
        self.current_input_port_id: Optional[PortIdentity] = None
        self.current_input_tuple_iter: Optional[Iterator[Tuple]] = None
        self.current_output_tuple: Optional[Tuple] = None
        self.current_internal_marker: Optional[InternalMarker] = None
        self.context_switch_condition: Condition = Condition()
        self.finished_current: Event = Event()

    def get_internal_marker(self) -> Optional[InternalMarker]:
        ret, self.current_internal_marker = self.current_internal_marker, None
        return ret

    def get_input_tuple(self) -> Optional[Tuple]:
        ret, self.current_input_tuple = self.current_input_tuple, None
        return ret

    def get_output_tuple(self) -> Optional[Tuple]:
        ret, self.current_output_tuple = self.current_output_tuple, None
        return ret

    def get_input_port_id(self) -> int:
        port_id = self.current_input_port_id
        # no upstream, special case for source executor.
        if port_id is None:
            return 0
        return port_id.id
