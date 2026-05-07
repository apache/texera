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

import pytest

from core.architecture.sendsemantics.broad_cast_partitioner import BroadcastPartitioner
from core.architecture.sendsemantics.hash_based_shuffle_partitioner import (
    HashBasedShufflePartitioner,
)
from core.architecture.sendsemantics.one_to_one_partitioner import OneToOnePartitioner
from core.architecture.sendsemantics.range_based_shuffle_partitioner import (
    RangeBasedShufflePartitioner,
)
from core.architecture.sendsemantics.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from core.models import Tuple
from core.models.schema.schema import Schema
from core.models.state import State
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessage,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    BroadcastPartitioning,
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    RangeBasedShufflePartitioning,
    RoundRobinPartitioning,
)

_HASHABLE_SCHEMA = Schema(raw_schema={"k": "INTEGER", "v": "STRING"})


def _worker(name: str) -> ActorVirtualIdentity:
    return ActorVirtualIdentity(name=name)


def _channel(src: str, dst: str) -> ChannelIdentity:
    return ChannelIdentity(from_worker_id=_worker(src), to_worker_id=_worker(dst))


def _tuple(**fields) -> Tuple:
    return Tuple(fields)


def _hashable_tuple(**fields) -> Tuple:
    return Tuple(fields, schema=_HASHABLE_SCHEMA)


def _snapshot(generator):
    # Several partitioners yield the receiver's pending batch by reference and
    # then clear it in the next statement of the generator. Snapshot list
    # payloads at yield time so tests see what the caller would see when
    # iterating tuple-by-tuple.
    out = []
    for item in generator:
        out.append(list(item) if isinstance(item, list) else item)
    return out


class TestBroadcastPartitioner:
    @pytest.fixture
    def partitioner(self):
        return BroadcastPartitioner(
            BroadcastPartitioning(
                batch_size=2,
                channels=[_channel("S", "A"), _channel("S", "B")],
            )
        )

    def test_init_collects_unique_receivers(self):
        p = BroadcastPartitioner(
            BroadcastPartitioning(
                batch_size=4,
                channels=[
                    _channel("S", "A"),
                    _channel("S", "B"),
                    _channel("S", "A"),
                ],
            )
        )
        assert p.batch_size == 4
        assert set(p.receivers) == {_worker("A"), _worker("B")}
        assert p.batch == []

    def test_add_tuple_below_batch_size_yields_nothing(self, partitioner):
        out = list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        assert out == []
        assert partitioner.batch == [_tuple(k=1)]

    def test_add_tuple_at_batch_size_emits_to_every_receiver_and_resets(
        self, partitioner
    ):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        out = list(partitioner.add_tuple_to_batch(_tuple(k=2)))
        emitted_receivers = {r for r, _ in out}
        assert emitted_receivers == {_worker("A"), _worker("B")}
        for _, batch in out:
            assert batch == [_tuple(k=1), _tuple(k=2)]
        assert partitioner.batch == []

    def test_flush_emits_pending_batch_and_ecm_only_to_target(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        ecm = EmbeddedControlMessage()
        out = list(partitioner.flush(_worker("A"), ecm))
        assert out == [[_tuple(k=1)], ecm]
        assert partitioner.batch == []

    def test_flush_with_empty_batch_emits_only_ecm_for_target(self, partitioner):
        ecm = EmbeddedControlMessage()
        out = list(partitioner.flush(_worker("A"), ecm))
        assert out == [ecm]

    def test_flush_to_non_receiver_emits_nothing(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        ecm = EmbeddedControlMessage()
        out = list(partitioner.flush(_worker("Z"), ecm))
        assert out == []

    def test_flush_state_emits_pending_batch_and_state_to_every_receiver(
        self, partitioner
    ):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        state = State()
        out = list(partitioner.flush_state(state))
        receivers_with_batch = [r for r, payload in out if payload == [_tuple(k=1)]]
        receivers_with_state = [r for r, payload in out if payload is state]
        assert set(receivers_with_batch) == {_worker("A"), _worker("B")}
        assert set(receivers_with_state) == {_worker("A"), _worker("B")}
        assert partitioner.batch == []

    def test_reset_clears_pending_batch(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        partitioner.reset()
        assert partitioner.batch == []


class TestRoundRobinPartitioner:
    @pytest.fixture
    def partitioner(self):
        return RoundRobinPartitioner(
            RoundRobinPartitioning(
                batch_size=2,
                channels=[_channel("S", "A"), _channel("S", "B"), _channel("S", "C")],
            )
        )

    def test_init_preserves_channel_order(self, partitioner):
        assert [r for r, _ in partitioner.receivers] == [
            _worker("A"),
            _worker("B"),
            _worker("C"),
        ]
        assert partitioner.round_robin_index == 0

    def test_init_dedupes_duplicate_channels_preserving_first_seen_order(self):
        p = RoundRobinPartitioner(
            RoundRobinPartitioning(
                batch_size=2,
                channels=[
                    _channel("S", "B"),
                    _channel("S", "A"),
                    _channel("S", "B"),
                ],
            )
        )
        assert [r for r, _ in p.receivers] == [_worker("B"), _worker("A")]

    def test_index_advances_modulo_receivers(self, partitioner):
        for tup in (_tuple(k=1), _tuple(k=2), _tuple(k=3), _tuple(k=4)):
            list(partitioner.add_tuple_to_batch(tup))
        # 4 tuples across 3 receivers, batch_size=2 → no batch reaches size 2 yet
        assert partitioner.round_robin_index == 1
        # one tuple landed in A's slot (index 0) twice (round 0 + round 3),
        # filling its batch and emitting on the second hit.
        # B has 1 (round 1), C has 1 (round 2).
        # We should not have seen any yield from B or C yet.

    def test_emits_batch_when_a_receiver_slot_fills(self, partitioner):
        outs = []
        for tup in (_tuple(k=1), _tuple(k=2), _tuple(k=3), _tuple(k=4)):
            outs.extend(list(partitioner.add_tuple_to_batch(tup)))
        # Tuple #4 lands in receiver A again (index 0) → batch [k=1, k=4] of size 2
        assert outs == [(_worker("A"), [_tuple(k=1), _tuple(k=4)])]

    def test_flush_emits_pending_batch_and_ecm_for_target_only(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))  # → A
        list(partitioner.add_tuple_to_batch(_tuple(k=2)))  # → B
        ecm = EmbeddedControlMessage()
        a_out = _snapshot(partitioner.flush(_worker("A"), ecm))
        assert a_out == [[_tuple(k=1)], ecm]
        # A's batch is now drained, B's pending batch remains untouched
        assert partitioner.receivers[1][1] == [_tuple(k=2)]

    def test_flush_to_unknown_receiver_emits_nothing(self, partitioner):
        ecm = EmbeddedControlMessage()
        assert list(partitioner.flush(_worker("Z"), ecm)) == []

    def test_flush_state_emits_pending_batches_and_state_for_each_receiver(
        self, partitioner
    ):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))  # → A
        list(partitioner.add_tuple_to_batch(_tuple(k=2)))  # → B
        state = State()
        out = []
        for receiver, payload in partitioner.flush_state(state):
            snap = list(payload) if isinstance(payload, list) else payload
            out.append((receiver, snap))
        # A and B emit (batch, state); C only emits state.
        assert (_worker("A"), [_tuple(k=1)]) in out
        assert (_worker("B"), [_tuple(k=2)]) in out
        assert (_worker("A"), state) in out
        assert (_worker("B"), state) in out
        assert (_worker("C"), state) in out


class TestHashBasedShufflePartitioner:
    def _partitioner(self, batch_size=10, hash_keys=("k",)):
        return HashBasedShufflePartitioner(
            HashBasedShufflePartitioning(
                batch_size=batch_size,
                channels=[_channel("S", "A"), _channel("S", "B")],
                hash_attribute_names=list(hash_keys),
            )
        )

    def test_same_key_routes_to_same_receiver_deterministically(self):
        p1 = self._partitioner()
        p2 = self._partitioner()
        # Drive each with the same tuple; routing is deterministic per process,
        # so two independent partitioners must place the tuple in the same slot.
        list(p1.add_tuple_to_batch(_hashable_tuple(k=42, v="x")))
        list(p2.add_tuple_to_batch(_hashable_tuple(k=42, v="x")))
        nonempty1 = [(r, b) for r, b in p1.receivers if b]
        nonempty2 = [(r, b) for r, b in p2.receivers if b]
        assert len(nonempty1) == 1
        assert nonempty1[0][0] == nonempty2[0][0]

    def test_full_batch_yields_and_clears_only_that_slot(self):
        p = self._partitioner(batch_size=2)
        outs = _snapshot(
            x
            for tup in (_hashable_tuple(k=7) for _ in range(5))
            for x in p.add_tuple_to_batch(tup)
        )
        assert len(outs) >= 1
        # After a yield the slot's batch is replaced with a fresh empty list,
        # so no receiver slot may exceed batch_size at any observation point.
        for _, batch in p.receivers:
            assert len(batch) < p.batch_size

    def test_no_hash_attribute_names_falls_back_to_whole_tuple(self):
        p = self._partitioner(hash_keys=())
        list(p.add_tuple_to_batch(_hashable_tuple(k=1, v="a")))
        list(p.add_tuple_to_batch(_hashable_tuple(k=2, v="b")))
        total = sum(len(b) for _, b in p.receivers)
        assert total == 2

    def test_flush_emits_pending_batch_and_ecm_for_target_only(self):
        p = self._partitioner(batch_size=10)
        # Force a tuple into receiver A regardless of hash outcome.
        p.receivers[0] = (p.receivers[0][0], [_hashable_tuple(k=1)])
        ecm = EmbeddedControlMessage()
        a_out = _snapshot(p.flush(p.receivers[0][0], ecm))
        assert a_out == [[_hashable_tuple(k=1)], ecm]

    def test_flush_state_emits_pending_batches_and_state(self):
        p = self._partitioner(batch_size=10)
        p.receivers[0] = (p.receivers[0][0], [_hashable_tuple(k=1)])
        state = State()
        out = []
        for receiver, payload in p.flush_state(state):
            snap = list(payload) if isinstance(payload, list) else payload
            out.append((receiver, snap))
        assert (p.receivers[0][0], [_hashable_tuple(k=1)]) in out
        # Each receiver still emits the state record.
        assert sum(1 for r, payload in out if payload is state) == len(p.receivers)


class TestRangeBasedShufflePartitioner:
    @pytest.fixture
    def partitioner(self):
        return RangeBasedShufflePartitioner(
            RangeBasedShufflePartitioning(
                batch_size=10,
                channels=[
                    _channel("S", "A"),
                    _channel("S", "B"),
                    _channel("S", "C"),
                ],
                range_attribute_names=["k"],
                range_min=0,
                range_max=9,
            )
        )

    def test_keys_per_receiver_partitions_range_evenly(self, partitioner):
        # (9 - 0) // 3 + 1 = 4
        assert partitioner.keys_per_receiver == 4

    def test_value_below_range_min_routes_to_first_receiver(self, partitioner):
        assert partitioner.get_receiver_index(-100) == 0

    def test_value_above_range_max_routes_to_last_receiver(self, partitioner):
        assert partitioner.get_receiver_index(10**6) == 2

    def test_value_in_range_routes_by_quotient(self, partitioner):
        # keys_per_receiver = 4 → indices: 0..3 → 0, 4..7 → 1, 8..9 (capped) → 2
        assert partitioner.get_receiver_index(0) == 0
        assert partitioner.get_receiver_index(3) == 0
        assert partitioner.get_receiver_index(4) == 1
        assert partitioner.get_receiver_index(7) == 1
        assert partitioner.get_receiver_index(8) == 2
        assert partitioner.get_receiver_index(9) == 2

    def test_add_tuple_routes_using_first_attribute(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=2)))
        list(partitioner.add_tuple_to_batch(_tuple(k=5)))
        list(partitioner.add_tuple_to_batch(_tuple(k=8)))
        receivers_to_batches = {r.name: b for r, b in partitioner.receivers}
        assert receivers_to_batches["A"] == [_tuple(k=2)]
        assert receivers_to_batches["B"] == [_tuple(k=5)]
        assert receivers_to_batches["C"] == [_tuple(k=8)]

    def test_full_batch_yields_and_clears_only_that_slot(self):
        p = RangeBasedShufflePartitioner(
            RangeBasedShufflePartitioning(
                batch_size=2,
                channels=[_channel("S", "A"), _channel("S", "B")],
                range_attribute_names=["k"],
                range_min=0,
                range_max=9,
            )
        )
        outs = []
        for v in (1, 2, 3):  # all route to receiver A (idx 0)
            outs.extend(list(p.add_tuple_to_batch(_tuple(k=v))))
        # First two tuples fill A's batch; second one yields and resets.
        assert outs == [(_worker("A"), [_tuple(k=1), _tuple(k=2)])]
        # A is now empty again, holding only the third tuple.
        assert p.receivers[0][1] == [_tuple(k=3)]

    def test_flush_emits_pending_batch_and_ecm_for_target_only(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=2)))  # → A
        list(partitioner.add_tuple_to_batch(_tuple(k=5)))  # → B
        ecm = EmbeddedControlMessage()
        a_out = _snapshot(partitioner.flush(_worker("A"), ecm))
        assert a_out == [[_tuple(k=2)], ecm]
        # B is untouched.
        assert partitioner.receivers[1][1] == [_tuple(k=5)]

    def test_flush_state_emits_pending_batches_and_state(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=2)))  # → A
        state = State()
        out = []
        for receiver, payload in partitioner.flush_state(state):
            snap = list(payload) if isinstance(payload, list) else payload
            out.append((receiver, snap))
        assert (_worker("A"), [_tuple(k=2)]) in out
        # Every receiver still emits the state, even with empty pending batch.
        assert sum(1 for r, payload in out if payload is state) == 3


class TestOneToOnePartitioner:
    @pytest.fixture
    def partitioner(self):
        return OneToOnePartitioner(
            OneToOnePartitioning(
                batch_size=2,
                channels=[
                    _channel("OTHER", "X"),
                    _channel("S", "A"),
                ],
            ),
            worker_id="S",
        )

    def test_init_picks_receiver_matching_worker_id(self, partitioner):
        assert partitioner.receiver == _worker("A")

    def test_add_tuple_below_batch_yields_nothing(self, partitioner):
        out = list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        assert out == []
        assert partitioner.batch == [_tuple(k=1)]

    def test_add_tuple_at_batch_yields_to_unique_receiver_and_resets(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        out = list(partitioner.add_tuple_to_batch(_tuple(k=2)))
        assert out == [(_worker("A"), [_tuple(k=1), _tuple(k=2)])]
        assert partitioner.batch == []

    def test_flush_emits_pending_batch_then_ecm(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        ecm = EmbeddedControlMessage()
        out = list(partitioner.flush(_worker("A"), ecm))
        assert out == [[_tuple(k=1)], ecm]
        assert partitioner.batch == []

    def test_flush_with_empty_batch_emits_only_ecm(self, partitioner):
        ecm = EmbeddedControlMessage()
        assert list(partitioner.flush(_worker("A"), ecm)) == [ecm]

    def test_flush_state_emits_pending_batch_then_state(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        state = State()
        out = list(partitioner.flush_state(state))
        assert out == [
            (_worker("A"), [_tuple(k=1)]),
            (_worker("A"), state),
        ]
        assert partitioner.batch == []

    def test_reset_clears_pending_batch(self, partitioner):
        list(partitioner.add_tuple_to_batch(_tuple(k=1)))
        partitioner.reset()
        assert partitioner.batch == []
