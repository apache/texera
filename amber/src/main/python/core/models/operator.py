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

import overrides
import pandas
from functools import lru_cache
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterator, List, Mapping, Optional, Union, MutableMapping, Protocol

from . import Table, TableLike, Tuple, TupleLike, Batch, BatchLike
from .state import State
from .table import all_output_to_tuple, table_from_ipc_bytes, table_to_ipc_bytes

import base64


class Operator(ABC):
    """
    Abstract base class for all operators.
    """

    class PythonTemplateDecoder:
        class Decoder(Protocol):
            """Pluggable base64 decoder interface."""

            def to_str(self, data: Union[str, bytes]) -> str: ...

        class StdlibBase64Decoder:
            """Default decoder using Python's stdlib base64."""

            def to_str(self, data: Union[str, bytes]) -> str:
                b64_bytes = data.encode("ascii") if isinstance(data, str) else data
                raw = base64.b64decode(b64_bytes, validate=False)
                return raw.decode("utf-8", errors="strict")

        def __init__(
            self,
            decoder: Optional["Operator.PythonTemplateDecoder.Decoder"] = None,
            cache_size: int = 256,
        ) -> None:
            self._decoder = decoder or self.StdlibBase64Decoder()
            self._decode_cached = self._build_cached_decoder(cache_size)

        def _build_cached_decoder(self, cache_size: int):
            @lru_cache(maxsize=cache_size)
            def _cached(data: Union[str, bytes]) -> str:
                return self._decoder.to_str(data)

            return _cached

        def decode(self, data: Union[str, bytes]) -> str:
            return self._decode_cached(data)

    def _get_template_decoder(self) -> "Operator.PythonTemplateDecoder":
        if not hasattr(self, "_python_template_decoder"):
            self._python_template_decoder = self.PythonTemplateDecoder(cache_size=256)
        return self._python_template_decoder

    def decode_python_template(self, data: Union[str, bytes]) -> str:
        return self._get_template_decoder().decode(data)

    __internal_is_source: bool = False

    @property
    @overrides.final
    def is_source(self) -> bool:
        """
        Whether the operator is a source operator. Source operators generate output
        Tuples without having input Tuples.

        :return:
        """
        return self.__internal_is_source

    @is_source.setter
    @overrides.final
    def is_source(self, value: bool) -> None:
        self.__internal_is_source = value

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass

    def process_state(self, state: State, port: int) -> Optional[State]:
        """
        Process an input State from the given link.
        The default implementation is to pass the State to all downstream operators.
        :param state: State, a State from an input port to be processed.
        :param port: int, input port index of the current exhausted port.
        :return: State, producing one State object
        """
        return state

    def produce_state_on_start(self, port: int) -> Optional[State]:
        """
        Produce a State when the given link started.

        :param port: int, input port index of the current initialized port.
        :return: State, producing one State object
        """
        pass

    def produce_state_on_finish(self, port: int) -> Optional[State]:
        """
        Produce a State after the input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: State, producing one State object
        """
        pass


class TupleOperatorV2(Operator):
    """
    Base class for tuple-oriented operators. A concrete implementation must
    be provided upon using.
    """

    @abstractmethod
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Callback when one input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield


class SourceOperator(TupleOperatorV2):
    _Operator__internal_is_source = True

    @abstractmethod
    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Tuples or Tables. Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        yield

    @overrides.final
    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        # TODO: change on_finish to output Iterator[Union[TupleLike, TableLike, None]]
        for i in self.produce():
            yield from all_output_to_tuple(i)

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield


class BatchOperator(TupleOperatorV2):
    """
    Base class for batch-oriented operators. A concrete implementation must
    be provided upon using.
    """

    BATCH_SIZE: int = 10  # must be a positive integer

    def __init__(self):
        super().__init__()
        self.__batch_data: MutableMapping[int, List[Tuple]] = defaultdict(list)
        self._validate_batch_size(self.BATCH_SIZE)

    @staticmethod
    @overrides.final
    def _validate_batch_size(value):
        if value is None:
            raise ValueError("BATCH_SIZE cannot be None.")
        if type(value) is not int:
            raise ValueError("BATCH_SIZE cannot be {type(value))}.")
        if value <= 0:
            raise ValueError("BATCH_SIZE should be positive.")

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        self.__batch_data[port].append(tuple_)
        if (
            self.BATCH_SIZE is not None
            and len(self.__batch_data[port]) >= self.BATCH_SIZE
        ):
            yield from self._process_batch(port)

    @overrides.final
    def _process_batch(self, port: int) -> Iterator[Optional[BatchLike]]:
        batch = Batch(
            pandas.DataFrame(
                [
                    self.__batch_data[port].pop(0).as_series()
                    for _ in range(min(len(self.__batch_data[port]), self.BATCH_SIZE))
                ]
            )
        )
        for output_batch in self.process_batch(batch, port):
            if output_batch is not None:
                if isinstance(output_batch, pandas.DataFrame):
                    # TODO: integrate into Batch as a helper function.
                    # convert from Batch to Tuple, only supports pandas.DataFrames for
                    # now.
                    for _, output_tuple in output_batch.iterrows():
                        yield output_tuple
                else:
                    yield output_batch

    @overrides.final
    def on_finish(self, port: int) -> Iterator[Optional[BatchLike]]:
        while len(self.__batch_data[port]) != 0:
            yield from self._process_batch(port)

    @abstractmethod
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        """
        Process an input Batch from the given link. The Batch is represented as a
        pandas.DataFrame.

        :param batch: Batch, a batch to be processed.
        :param port: int, input port index of the current Batch.
        :return: Iterator[Optional[BatchLike]], producing one BatchLike object at a
            time, or None.
        """
        yield


class TableOperator(TupleOperatorV2):
    """
    Base class for table-oriented operators. A concrete implementation must
    be provided upon using.
    """

    def __init__(self):
        super().__init__()
        self._Operator__internal_is_source: bool = False
        self.__table_data: Mapping[int, List[Tuple]] = defaultdict(list)

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        self.__table_data[port].append(tuple_)
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TableLike]]:
        table = Table(self.__table_data[port])
        yield from self.process_table(table, port)

    def _buffered_table(self, port: int) -> Table:
        """Tuples buffered for ``port`` so far, materialized as a Table.

        Exposed so subclasses (e.g. ``LoopStartOperator``) can read the
        buffer outside the ``process_table`` callback without reaching into
        the parent's name-mangled private field. Inside this class
        ``self.__table_data`` resolves via normal name mangling, so a future
        rename of ``TableOperator`` keeps callers transparent.
        """
        return Table(self.__table_data[port])

    @abstractmethod
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as a
        pandas.DataFrame.

        :param table: Table, a table to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a
            time, or None.
        """
        yield


# Names the loop runtime owns inside the eval namespaces and across the loop
# boundary; explicitly stripped from every state dict that crosses Loop
# Start / Loop End so user code can neither read nor persist them.
# Other reserved names that used to live in user state -- ``loop_counter``,
# ``LoopStartId``, ``LoopStartStateURI`` -- are no longer in ``self.state``
# at all; they ride the StateFrame envelope (see ``core.models.payload``)
# and are stamped/captured by ``MainLoop._process_state_frame``.
_RESERVED_STATE_KEYS: frozenset = frozenset({"table", "output"})


class LoopStartOperator(TableOperator):
    """Base class for the runtime side of a Loop Start operator.

    The generator in ``LoopStartOpDesc.scala`` emits a thin
    ``ProcessLoopStartOperator(LoopStartOperator)`` subclass that does
    nothing more than wire the user-supplied ``initialization`` and
    ``output`` expressions into ``open()`` and ``process_table()``; all
    substantive logic lives here.

    Lifecycle
    ---------
    * ``open()`` runs once when the worker starts. The generated subclass
      executes the user's ``initialization`` against a fresh ``self.state``
      dict; after it returns ``self.state`` holds *only* the user's loop
      variables.
    * ``process_state(state, port)`` (final) runs once when upstream sends
      this LoopStart its input state; it merges that state into
      ``self.state``. The nested pass-through branch and all
      ``loop_counter`` bookkeeping live in
      ``MainLoop._process_state_frame``, not here.
    * ``process_table(table, port)`` is provided by the generated subclass
      and yields a downstream row via ``eval_output(...)`` against the
      user's ``output`` expression.
    * ``produce_state_on_finish(port)`` (final) emits the state crossing
      the boundary to the matching LoopEnd: user variables plus the input
      table serialized as an Apache Arrow IPC stream (not pickle).

    Subclass contract
    -----------------
    The generated subclass overrides ``open()`` and ``process_table()``
    only. All other methods are ``@overrides.final``; do not override
    them. After ``open()`` returns, ``self.state`` must be a dict
    containing the user's loop variables (none of the reserved names in
    ``_RESERVED_STATE_KEYS``).

    Reserved names
    --------------
    * ``loop_counter`` / ``LoopStartId`` / ``LoopStartStateURI`` -- live on
      the StateFrame envelope (``core.models.payload``), not in
      ``self.state``. Stamped by this operator's worker via
      ``MainLoop._compute_loop_start_id``.
    * ``table`` / ``output`` -- transient names only available inside the
      ``eval_output`` throwaway namespace; never persisted in
      ``self.state``. See ``_RESERVED_STATE_KEYS``.
    """

    @overrides.final
    def process_state(self, state: State, port: int) -> Optional[State]:
        # First-entry only: merge upstream state into self.state. The nested
        # pass-through (state already carrying LoopStartStateURI) and all
        # loop_counter bookkeeping are owned by the worker runtime
        # (main_loop._process_state_frame), so this operator never sees the
        # counter and never mutates the State it is handed.
        self.state.update(state)
        return None

    @overrides.final
    def eval_output(self, output_expr: str, table: Table) -> TableLike:
        # Run the user's `output` expression in a throwaway namespace seeded
        # with the loop variables and the input `table`. This lets user code
        # read `table` and define `output` without those reserved names leaking
        # into -- or being silently clobbered out of -- the persistent loop
        # state (self.state), addressing the exec-namespace collision.
        namespace = {**self.state, "table": table}
        exec("output = " + output_expr, {}, namespace)
        return namespace["output"]

    @overrides.final
    def produce_state_on_finish(self, port: int) -> State:
        # Emit the user's loop variables plus the buffered input table for the
        # matching LoopEnd. The table rides as an Apache Arrow IPC stream, not
        # pickle bytes: the receiving LoopEnd would otherwise have to
        # `pickle.loads` data that lives in iceberg, a remote-code-execution
        # surface. `table`/`output` are runtime-reserved and are not kept in
        # self.state, so drop any stray ones before adding the real table.
        # Reads the buffer through `_buffered_table` so a rename of
        # `TableOperator` doesn't silently break this.
        produced = {
            key: value
            for key, value in self.state.items()
            if key not in _RESERVED_STATE_KEYS
        }
        produced["table"] = table_to_ipc_bytes(self._buffered_table(port))
        return produced


class LoopEndOperator(TableOperator):
    """Base class for the runtime side of a Loop End operator.

    The generator in ``LoopEndOpDesc.scala`` emits a thin
    ``ProcessLoopEndOperator(LoopEndOperator)`` subclass that wires the
    user-supplied ``update`` expression into ``process_state(...)`` (via
    ``run_update``) and the ``condition`` expression into ``condition()``
    (via ``eval_condition``); all substantive logic lives here.

    Lifecycle
    ---------
    * ``process_table(table, port)`` (final) yields each input table
      through as-is.
    * ``process_state(state, port)`` is provided by the generated
      subclass. It calls ``run_update(update_code, state)`` to decode the
      input table (from its Arrow IPC bytes), run the user's ``update`` in
      a throwaway namespace, stash the table on ``self._loop_table``, and
      persist only user variables back into ``self.state``. Returns
      ``None``.
    * ``condition()`` is the abstract method the generated subclass
      implements by delegating to ``eval_condition(...)`` against the
      user's ``condition`` expression. Called by ``MainLoop.complete()``
      to decide whether to fire the back-edge via
      ``_jump_to_loop_start``.

    Subclass contract
    -----------------
    The generated subclass overrides ``process_state()`` (delegating to
    ``run_update``) and ``condition()`` (delegating to
    ``eval_condition``). All other methods are ``@overrides.final``; do
    not override them.

    Reserved names
    --------------
    Same as ``LoopStartOperator``: ``loop_counter`` / ``LoopStartId`` /
    ``LoopStartStateURI`` live on the StateFrame envelope (never in user
    state); ``table`` / ``output`` are transient names available only
    inside ``run_update`` / ``eval_condition``'s throwaway namespace and
    are stripped from ``self.state``. See ``_RESERVED_STATE_KEYS``.
    """

    @overrides.final
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table

    @overrides.final
    def run_update(self, update_code: str, state: State) -> None:
        # Run the user's `update` in a throwaway namespace seeded with the
        # incoming loop variables and the input table, then persist only the
        # user variables back into self.state. The table arrives as an Apache
        # Arrow IPC stream (see LoopStartOperator.produce_state_on_finish), so
        # it is decoded structurally rather than via pickle.loads -- no
        # remote-code-execution surface. `table`/`output` are runtime-reserved
        # and never persist, so user code cannot silently clobber loop
        # machinery through them. The real input table is kept on
        # self._loop_table so condition() can read it after the update.
        table = table_from_ipc_bytes(state["table"])
        namespace = {
            key: value
            for key, value in state.items()
            if key not in _RESERVED_STATE_KEYS
        }
        namespace["table"] = table
        exec(update_code, {}, namespace)
        self._loop_table = table
        self.state = {
            key: value
            for key, value in namespace.items()
            if key not in _RESERVED_STATE_KEYS
        }

    @overrides.final
    def eval_condition(self, condition_expr: str) -> bool:
        namespace = {**self.state, "table": self._loop_table}
        exec("output = " + condition_expr, {}, namespace)
        return namespace["output"]

    @abstractmethod
    def condition(self) -> bool:
        pass
