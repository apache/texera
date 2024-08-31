from dataclasses import dataclass


@dataclass
class Marker:
    pass


@dataclass
class EndOfUpstream(Marker):
    pass
