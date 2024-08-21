from typing import Iterator, Optional, Union, Dict, List

from core.models import Tuple, ArrowTableTupleProvider, Schema, InputExhausted
from core.models.internal_marker import EndOfAll, InternalMarker, SenderChange
from core.models.marker import EndOfUpstream
from core.models.payload import DataFrame, DataPayload, MarkerFrame
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    PortIdentity,
    ChannelIdentity,
)


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

    def __init__(self):
        self._ports: Dict[PortIdentity, WorkerPort] = dict()
        self._channels: Dict[ChannelIdentity, Channel] = dict()
        self._current_channel_id: Optional[ChannelIdentity] = None

    def add_input_port(self, port_id: PortIdentity, schema: Schema) -> None:
        if port_id.id is None:
            port_id.id = 0
        if port_id.internal is None:
            port_id.internal = False

        # each port can only be added and initialized once.
        if port_id not in self._ports:
            self._ports[port_id] = WorkerPort(schema)

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
        self, from_: ActorVirtualIdentity, payload: DataPayload
    ) -> Iterator[Union[Tuple, InputExhausted, InternalMarker]]:
        # special case used to yield for source op
        if from_ == InputManager.SOURCE_STARTER:
            yield InputExhausted()
            yield EndOfAll()
            return
        current_channel_id = None
        for channel_id, channel in self._channels.items():
            if channel_id.from_worker_id == from_:
                current_channel_id = channel_id

        if (
            self._current_channel_id is None
            or self._current_channel_id != current_channel_id
        ):
            self._current_channel_id = current_channel_id
            yield SenderChange(current_channel_id)

        if isinstance(payload, DataFrame):
            for field_accessor in ArrowTableTupleProvider(payload.frame):
                yield Tuple(
                    {name: field_accessor for name in payload.frame.column_names},
                    schema=self._ports[
                        self._channels[self._current_channel_id].port_id
                    ].get_schema(),
                )

        elif isinstance(payload, MarkerFrame) and isinstance(
            payload.frame, EndOfUpstream
        ):
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
                yield InputExhausted()

            all_ports_completed = all(
                map(lambda port: port.is_completed(), self._ports.values())
            )

            if all_ports_completed:
                yield EndOfAll()

        else:
            raise NotImplementedError()
