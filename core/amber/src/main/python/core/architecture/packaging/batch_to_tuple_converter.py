from collections import defaultdict
from typing import Iterator, Optional, Set, Union, Dict

from core.models import Tuple, ArrowTableTupleProvider
from core.models.marker import EndOfAllMarker, Marker, SenderChangeMarker
from core.models.payload import InputDataFrame, DataPayload, EndOfUpstream
from core.models.tuple import InputExhausted
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, LinkIdentity


class BatchToTupleConverter:
    SOURCE_STARTER = ActorVirtualIdentity("SOURCE_STARTER")

    def __init__(self):
        self._input_map: Dict[ActorVirtualIdentity, LinkIdentity] = dict()
        self._upstream_map: defaultdict[
            LinkIdentity, Set[ActorVirtualIdentity]
        ] = defaultdict(set)
        self._current_link: Optional[LinkIdentity] = None

    def register_input(
        self, identifier: ActorVirtualIdentity, input_: LinkIdentity
    ) -> None:
        self._upstream_map[input_].add(identifier)
        self._input_map[identifier] = input_

    def process_data_payload(
        self, from_: ActorVirtualIdentity, payload: DataPayload
    ) -> Iterator[Union[Tuple, InputExhausted, Marker]]:
        # special case used to yield for source op
        if from_ == BatchToTupleConverter.SOURCE_STARTER:
            yield InputExhausted()
            yield EndOfAllMarker()
            return

        link = self._input_map[from_]

        if self._current_link is None or self._current_link != link:
            self._current_link = link
            yield SenderChangeMarker(link)

        if isinstance(payload, InputDataFrame):
            for field_accessor in ArrowTableTupleProvider(payload.frame):
                yield Tuple(
                    {name: field_accessor for name in payload.frame.column_names}
                )

        elif isinstance(payload, EndOfUpstream):
            self._upstream_map[link].remove(from_)
            if len(self._upstream_map[link]) == 0:
                del self._upstream_map[link]
                yield InputExhausted()
            if len(self._upstream_map) == 0:
                yield EndOfAllMarker()

        else:
            raise NotImplementedError()
