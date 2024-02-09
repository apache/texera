from dataclasses import dataclass


from proto.edu.uci.ics.amber.engine.common import ChannelIdentity


@dataclass
class Marker:
    """
    A special Data Message, only being generated in un-packaging a batch into Tuples.
    Markers retain the order information and served as a indicator of data state.
    """

    pass


@dataclass
class SenderChangeMarker(Marker):
    channel_id: ChannelIdentity


@dataclass
class EndOfAllMarker(Marker):
    pass
