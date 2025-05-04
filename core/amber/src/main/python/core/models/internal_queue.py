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

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import TypeVar, Set

from core.models.internal_marker import InternalMarker
from core.models.payload import DataPayload
from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue,
)
from core.util.customized_queue.queue_base import IQueue, QueueElement
from proto.edu.uci.ics.amber.core import ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import ChannelMarkerPayload
from proto.edu.uci.ics.amber.engine.common import ControlPayloadV2


@dataclass
class InternalQueueElement(QueueElement):
    tag: ChannelIdentity


@dataclass
class DataElement(InternalQueueElement):
    payload: DataPayload


@dataclass
class ControlElement(InternalQueueElement):
    payload: ControlPayloadV2


@dataclass
class ChannelMarkerElement(InternalQueueElement):
    payload: ChannelMarkerPayload


T = TypeVar("T", bound=InternalQueueElement)


class InternalQueue(IQueue):

    class DisableType(Enum):
        DISABLE_BY_PAUSE = 1
        DISABLE_BY_BACKPRESSURE = 2

    def __init__(self):
        self._queue = LinkedBlockingMultiQueue()
        self._queue.add_sub_queue("SYSTEM", 0)
        self._queue_ids: Set[ChannelIdentity] = set()
        self._queue_state: Set[InternalQueue.DisableType] = set()
        self._lock = RLock()

    def is_empty(self, key=None) -> bool:
        return self._queue.is_empty(key)

    def get(self) -> T:
        return self._queue.get()

    def put(self, item: T) -> None:
        if isinstance(item, InternalQueueElement):
            if item.tag not in self._queue_ids:
                self._queue.add_sub_queue(item.tag, 1 if item.tag.is_control else 2)
                self._queue_ids.add(item.tag)
            if isinstance(item, (DataElement, InternalMarker, ChannelMarkerElement)):
                self._queue.put(item.tag, item)
            elif isinstance(item, ControlElement):
                self._queue.put(item.tag, item)
            else:
                raise ValueError(f"item {item} is not recognized by internal queue")
        else:
            self._queue.put("SYSTEM", item)

    def disable(self, channel_id: ChannelIdentity) -> None:
        self._queue.disable(channel_id)

    def enable(self, channel_id: ChannelIdentity) -> None:
        self._queue.enable(channel_id)

    def is_control_empty(self) -> bool:
        return all(
            self.is_empty(queue_id)
            for queue_id in self._queue_ids
            if queue_id.is_control
        )

    def is_data_empty(self) -> bool:
        return all(
            self.is_empty(queue_id)
            for queue_id in self._queue_ids
            if not queue_id.is_control
        )

    def __len__(self) -> int:
        return self.size()

    def size(self) -> int:
        return self._queue.size()

    def size_control(self) -> int:
        return sum(
            self._queue.size(queue_id)
            for queue_id in self._queue_ids
            if queue_id.is_control
        )

    def size_data(self) -> int:
        return sum(
            self._queue.size(queue_id)
            for queue_id in self._queue_ids
            if not queue_id.is_control
        )

    def enable_data(self, disable_type: DisableType) -> bool:
        with self._lock:
            if disable_type in self._queue_state:
                self._queue_state.remove(disable_type)
            if self._queue_state:
                return False
            for queue_id in self._queue_ids:
                if not queue_id.is_control:
                    self._queue.enable(queue_id)
            return True

    def disable_data(self, disable_type: DisableType) -> None:
        with self._lock:
            self._queue_state.add(disable_type)
            for queue_id in self._queue_ids:
                if not queue_id.is_control:
                    self._queue.disable(queue_id)

    def in_mem_size(self) -> int:
        return sum(
            self._queue.in_mem_size(queue_id)
            for queue_id in self._queue_ids
            if not queue_id.is_control
        )

    def is_data_enabled(self) -> bool:
        return any(
            self._queue.is_enabled(queue_id)
            for queue_id in self._queue_ids
            if not queue_id.is_control
        )
