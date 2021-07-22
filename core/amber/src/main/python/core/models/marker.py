from dataclasses import dataclass

from proto.edu.uci.ics.amber.engine.common import LinkIdentity


@dataclass
class Marker:
    pass


@dataclass
class SenderChangeMarker(Marker):
    link: LinkIdentity


@dataclass
class EndMarker(Marker):
    pass


@dataclass
class EndOfAllMarker(Marker):
    pass
