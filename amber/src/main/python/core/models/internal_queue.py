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
from typing import Tuple, TypeVar, Set

from core.models.payload import DataPayload
from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue,
)
from core.util.customized_queue.queue_base import IQueue, QueueElement
from proto.org.apache.texera.amber.core import ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import EmbeddedControlMessage
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2


@dataclass
class InternalQueueElement(QueueElement):
    tag: ChannelIdentity


@dataclass
class DataElement(InternalQueueElement):
    payload: DataPayload


@dataclass
class DCMElement(InternalQueueElement):
    payload: DirectControlMessagePayloadV2


@dataclass
class ECMElement(InternalQueueElement):
    payload: EmbeddedControlMessage


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
        """Blocking get of the next available element.

        Data channels register enabled even during a disable window, because
        ECMs ride data channels and one swallowed by a channel that came up
        disabled would never be acked. A DataElement arriving here during
        such a window is withheld instead: its channel is closed and the
        element goes back to its sub-queue's head, for enable_data() to
        release. An ECM queued behind it on the same channel is therefore
        delayed until resume, which is unavoidable without unbounded
        buffering, and is what main does for channels disable_data() closed.

        Assumes a single consumer; only PauseManager and BackpressureHandler
        toggle the disable state, and only on the input queue.
        """
        while True:
            item = self._queue.get()
            # a control-tagged DataElement cannot occur today, but the tag is
            # wire-derived: withholding one would close a control sub-queue,
            # which enable_data() never reopens
            if (
                not isinstance(item, DataElement)
                or item.tag.is_control
                or not self._queue_state
            ):
                return item
            with self._lock:
                # enable_data() may have cleared the last reason since the
                # check above; closing the channel now would strand it
                if not self._queue_state:
                    return item
                # disable first: the element must never be dequeuable in between
                self._queue.disable(item.tag)
                self._queue.put_first(item.tag, item)

    def put(self, item: T) -> None:
        if isinstance(item, InternalQueueElement):
            if item.tag not in self._queue_ids:
                # both the lock and the re-check are load-bearing:
                # disable_data/enable_data iterate _queue_ids live, and a
                # second add_sub_queue for the same channel would replace its
                # sub-queue with an empty one, dropping whatever it holds
                with self._lock:
                    if item.tag not in self._queue_ids:
                        self._queue.add_sub_queue(
                            item.tag, 1 if item.tag.is_control else 2
                        )
                        self._queue_ids.add(item.tag)
            if isinstance(item, (DataElement, ECMElement, DCMElement)):
                self._queue.put(item.tag, item)
            else:
                raise ValueError(f"item {item} is not recognized by internal queue")
        else:
            self._queue.put("SYSTEM", item)

    def disable(self, channel_id: ChannelIdentity) -> None:
        self._queue.disable(channel_id)

    def enable(self, channel_id: ChannelIdentity) -> None:
        self._queue.enable(channel_id)

    def _control_queue_ids(self) -> Tuple[ChannelIdentity, ...]:
        """Snapshot of the registered control channels.

        put() can grow _queue_ids from another thread, and iterating the
        live set while it grows raises RuntimeError, so queries must iterate
        a snapshot taken through these helpers.
        """
        snapshot = tuple(self._queue_ids)
        return tuple(queue_id for queue_id in snapshot if queue_id.is_control)

    def _data_queue_ids(self) -> Tuple[ChannelIdentity, ...]:
        """Snapshot of the registered data channels; see _control_queue_ids."""
        snapshot = tuple(self._queue_ids)
        return tuple(queue_id for queue_id in snapshot if not queue_id.is_control)

    def is_control_empty(self) -> bool:
        return all(self.is_empty(queue_id) for queue_id in self._control_queue_ids())

    def is_data_empty(self) -> bool:
        return all(self.is_empty(queue_id) for queue_id in self._data_queue_ids())

    def __len__(self) -> int:
        return self.size()

    def size(self) -> int:
        return self._queue.size()

    def size_control(self) -> int:
        return sum(self._queue.size(queue_id) for queue_id in self._control_queue_ids())

    def size_data(self) -> int:
        return sum(self._queue.size(queue_id) for queue_id in self._data_queue_ids())

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
            self._queue.in_mem_size(queue_id) for queue_id in self._data_queue_ids()
        )

    def is_data_enabled(self) -> bool:
        # channels registered mid-disable come up enabled (see get()), so
        # per-channel state alone would report data as enabled during a pause
        # and let main_loop's wait-loop exit and resume processing
        if self._queue_state:
            return False
        return any(
            self._queue.is_enabled(queue_id) for queue_id in self._data_queue_ids()
        )
