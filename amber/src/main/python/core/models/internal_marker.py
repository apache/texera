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


from dataclasses import dataclass


class InternalMarker:
    """
    A special Data Message, only being generated in un-packaging a batch into Tuples.
    Markers retain the order information and served as a indicator of data state.
    """

    pass


@dataclass(frozen=True)
class PortMarker(InternalMarker):
    """A control marker bound to the input port that produced it."""

    port_id: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.port_id, int)
            or isinstance(self.port_id, bool)
            or self.port_id < 0
        ):
            raise ValueError("marker port_id must be a nonnegative integer")


class StartChannel(PortMarker):
    """Start-of-channel marker with immutable input-port provenance."""


class EndChannel(PortMarker):
    """End-of-channel marker with immutable input-port provenance."""
