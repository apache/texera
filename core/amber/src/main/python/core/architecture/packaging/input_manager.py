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

import threading
from typing import Iterator, Optional, Union, Dict, List
from pyarrow.lib import Table
from core.models import Tuple, ArrowTableTupleProvider, Schema, InternalQueue
from core.models.internal_marker import (
    InternalMarker,
    StartOfOutputPorts,
    EndOfOutputPorts,
    EndOfInputPort,
    StartOfInputPort,
)
from core.models.marker import EndOfInputChannel, State, StartOfInputChannel, Marker
from core.models.payload import DataFrame, DataPayload, MarkerFrame
from core.storage.runnables.input_port_materialization_reader_runnable import (
    InputPortMaterializationReaderRunnable,
)
from proto.edu.uci.ics.amber.core import (
    ActorVirtualIdentity,
    PortIdentity,
    ChannelIdentity,
)
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning


class Channel:
    def __init__(self):
        self.port_id: Optional[PortIdentity] = None
        self.completed = False

    def set_port_id(self, port_id: PortIdentity) -> None:
        self.port_id = port_id

    def complete(self) -> None:
        self.completed = True

    def is_completed(self) -> bool:
        return self.completed


class WorkerPort:
    def __init__(self, schema: Schema):
        self.channels: List[Channel] = list()
        self._schema = schema

    def add_channel(self, channel: Channel) -> None:
        self.channels.append(channel)

    def is_completed(self) -> bool:
        return bool(self.channels) and all(
            map(lambda channel: channel.is_completed(), self.channels)
        )

    def get_schema(self) -> Schema:
        return self._schema


class InputManager:
    SOURCE_STARTER = ActorVirtualIdentity("SOURCE_STARTER")

    def __init__(self, worker_id: str, input_queue: InternalQueue):
        self.worker_id = worker_id
        self._ports: Dict[PortIdentity, WorkerPort] = dict()
        self._channels: Dict[ChannelIdentity, Channel] = dict()
        self._current_channel_id: Optional[ChannelIdentity] = None
        self.started = False
        self._input_queue = input_queue
        self._input_port_mat_reader_runnables: Dict[
            PortIdentity, List[InputPortMaterializationReaderRunnable]
        ] = dict()

    def set_up_input_port_mat_reader_threads(
        self, port_id: PortIdentity, uris: List[str], partitionings: List[Partitioning]
    ) -> None:
        assert len(uris) == len(partitionings)
        if uris is not None:
            reader_runnables = [
                InputPortMaterializationReaderRunnable(
                    uri=uri,
                    queue=self._input_queue,
                    worker_actor_id=ActorVirtualIdentity(self.worker_id),
                    partitioning=partitioning,
                )
                for uri, partitioning in zip(uris, partitionings)
            ]
            self._input_port_mat_reader_runnables[port_id] = reader_runnables

    def get_input_port_mat_reader_threads(
        self,
    ) -> Dict[PortIdentity, List[InputPortMaterializationReaderRunnable]]:
        return self._input_port_mat_reader_runnables

    def start_input_port_mat_reader_threads(self):
        for port_reader_runnables in self._input_port_mat_reader_runnables.values():
            for reader_runnable in port_reader_runnables:
                thread_for_reader_runnable = threading.Thread(
                    target=reader_runnable.run,
                    daemon=True,
                    name=f"port_mat_reader_runnable_thread_"
                    f"{reader_runnable.channel_id}",
                )
                thread_for_reader_runnable.start()

    def get_all_channel_ids(self) -> Dict["ChannelIdentity", "Channel"].keys:
        return self._channels.keys()

    def add_input_port(
        self,
        port_id: PortIdentity,
        schema: Schema,
        storage_uris: List[str],
        partitionings: List[Partitioning],
    ) -> None:
        if port_id.id is None:
            port_id.id = 0
        if port_id.internal is None:
            port_id.internal = False

        # each port can only be added and initialized once.
        if port_id not in self._ports:
            self._ports[port_id] = WorkerPort(schema)

        self.set_up_input_port_mat_reader_threads(port_id, storage_uris, partitionings)

    def get_port_id(self, channel_id: ChannelIdentity) -> PortIdentity:
        return self._channels[channel_id].port_id

    def register_input(
        self, channel_id: ChannelIdentity, port_id: PortIdentity
    ) -> None:
        if port_id.id is None:
            port_id.id = 0
        if port_id.internal is None:
            port_id.internal = False
        channel = Channel()
        channel.set_port_id(port_id)
        self._channels[channel_id] = channel
        self._ports[port_id].add_channel(channel)

    def process_data_payload(
        self, from_: ChannelIdentity, payload: DataPayload
    ) -> Iterator[Union[Tuple, InternalMarker]]:

        self._current_channel_id = from_

        # special case used to yield for source op
        if from_.from_worker_id == InputManager.SOURCE_STARTER:
            yield EndOfInputPort()
            yield EndOfOutputPorts()
            return

        if isinstance(payload, DataFrame):
            yield from self._process_data(payload.frame)
        elif isinstance(payload, MarkerFrame):
            yield from self._process_marker(payload.frame)
        else:
            raise NotImplementedError()

    def _process_data(self, table: Table) -> Iterator[Tuple]:
        schema = self._ports[
            self._channels[self._current_channel_id].port_id
        ].get_schema()
        for field_accessor in ArrowTableTupleProvider(table):
            yield Tuple(
                {name: field_accessor for name in table.column_names}, schema=schema
            )

    def _process_marker(self, marker: Marker) -> Iterator[InternalMarker]:
        if isinstance(marker, State):
            yield marker
        if isinstance(marker, StartOfInputChannel):
            if not self.started:
                yield StartOfOutputPorts()
            self.started = True
            yield StartOfInputPort()
        if isinstance(marker, EndOfInputChannel):
            channel = self._channels[self._current_channel_id]
            channel.complete()
            port_id = channel.port_id
            port_completed = all(
                map(
                    lambda channel: channel.is_completed(),
                    self._ports[port_id].channels,
                )
            )

            if port_completed:
                yield EndOfInputPort()

            all_ports_completed = all(
                map(lambda port: port.is_completed(), self._ports.values())
            )

            if all_ports_completed:
                yield EndOfOutputPorts()
