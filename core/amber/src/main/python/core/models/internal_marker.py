from dataclasses import dataclass
from core.models.marker import Marker


@dataclass
class InternalMarker(Marker):
    """
    A special Data Message, only being generated in un-packaging a batch into Tuples.
    Markers retain the order information and served as a indicator of data state.
    """

    pass


@dataclass
class StartOfInputPort(InternalMarker):
    pass


@dataclass
class EndOfInputPort(InternalMarker):
    pass


@dataclass
class StartOfOutputPorts(InternalMarker):
    pass


@dataclass
class EndOfOutputPorts(InternalMarker):
    pass
