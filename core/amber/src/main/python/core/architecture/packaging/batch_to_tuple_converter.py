from collections import defaultdict

from typing import Iterator, Set, Union

from core.models import Tuple
from core.models.marker import EndMarker, EndOfAllMarker, Marker, SenderChangeMarker
from core.models.payload import DataFrame, DataPayload, EndOfUpstream
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, LinkIdentity


class BatchToTupleConverter:
    def __init__(self):
        self._input_map: dict[ActorVirtualIdentity, LinkIdentity] = dict()
        self._upstream_map: defaultdict[LinkIdentity, Set[ActorVirtualIdentity]] = defaultdict(set)
        self._current_link: LinkIdentity = None

    def register_input(self, identifier: ActorVirtualIdentity, input_: LinkIdentity) -> None:
        self._upstream_map[input_].add(identifier)
        self._input_map[identifier] = input_

    def process_data_payload(self, from_: ActorVirtualIdentity, data_payload: DataPayload) \
            -> Iterator[Union[Tuple, Marker]]:
        link = self._input_map[from_]
        if self._current_link is None or self._current_link != link:
            self._current_link = link
            yield SenderChangeMarker(link)

        if isinstance(data_payload, DataFrame):
            for tuple_ in data_payload.frame:
                yield tuple_

        elif isinstance(data_payload, EndOfUpstream):
            self._upstream_map[link].remove(from_)
            if len(self._upstream_map[link]) == 0:
                del self._upstream_map[link]
                yield EndMarker()
            if len(self._upstream_map) == 0:
                yield EndOfAllMarker()

        else:
            raise NotImplementedError()
