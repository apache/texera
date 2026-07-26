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


# ``table`` is the loop's input table, seeded by the runtime into the eval/exec
# namespaces the loop expressions run in. It is NOT user state: a user loop
# variable of the same name collides with it, so both operators raise on
# collision (see ``_reserved_name_error``) rather than silently dropping the
# user's value. The envelope names (``loop_counter`` / ``loop_start_id``) never
# enter user state -- they ride the StateFrame envelope (see
# ``core.models.payload``). The loop-back write address is setup config, not
# state (see ``loopStartStateUris`` on the ``InitializeExecutorRequest`` proto).
_TABLE_KEY = "table"
_RESERVED_STATE_KEYS: frozenset = frozenset({_TABLE_KEY})


def _reserved_name_error(name: str) -> ValueError:
    return ValueError(
        f"'{name}' is reserved by the loop runtime (it is the loop's input "
        f"table); rename the loop variable."
    )


def _strip_reserved(state: State) -> State:
    """Return ``state`` without the runtime-reserved keys (``_RESERVED_STATE_KEYS``)."""
    return State(
        {key: value for key, value in state.items() if key not in _RESERVED_STATE_KEYS}
    )


def _eval_loop_expr(expr: str, state: State, table: Optional[Table]):
    """Evaluate ``expr`` directly against the loop variables plus ``table``.

    Runs in a throwaway namespace seeded with the loop variables and ``table``
    so the seeded ``table`` neither leaks into nor is clobbered out of the
    persistent loop ``state``. Shared by LoopStart's ``output`` expression and
    LoopEnd's ``condition``.

    The namespace is passed as ``eval`` globals (not a locals-only mapping):
    a generator expression / comprehension / lambda in the user's expression
    resolves its free variables against globals, so a locals-only namespace
    would raise ``NameError`` on otherwise-valid expressions like
    ``all(x > threshold for x in items)``. The namespace is discarded, so the
    ``__builtins__`` that ``eval`` injects into it does not matter here.
    """
    namespace = {**state, _TABLE_KEY: table}
    return eval(expr, namespace)


class LoopStartOperator(TableOperator):
    """Base class for the runtime side of a Loop Start operator.

    The generator in ``LoopStartOpDesc.scala`` emits a thin
    ``ProcessLoopStartOperator(LoopStartOperator)`` subclass that wires the
    user-supplied ``initialization`` and ``output`` expressions into
    ``open()`` and ``process_table()``; all substantive logic lives here.

    ``open()`` seeds ``self.state`` with the user's loop variables;
    ``process_state`` merges upstream state in; ``produce_state_on_finish``
    emits those variables plus the input table (Arrow IPC; see
    ``table_to_ipc_bytes`` in ``core.models.table``) to the matching LoopEnd.
    ``loop_counter`` and the nested pass-through are owned by
    ``MainLoop._process_state_frame``, not this operator.

    Subclass contract: the generated subclass overrides ``open()`` and
    ``process_table()`` only; all other methods are ``@overrides.final``. After
    ``open()`` returns ``self.state`` holds only the user's loop variables --
    not the reserved ``table``; see the ``_RESERVED_STATE_KEYS`` module comment
    for the ``table`` vs envelope-borne counter/id split.
    """

    @overrides.final
    def process_state(self, state: State, port: int) -> Optional[State]:
        # First-entry only: merge upstream state into self.state. The nested
        # pass-through (a frame already stamped with a LoopStartId) and all
        # loop_counter bookkeeping are owned by the worker runtime
        # (main_loop._process_state_frame), so this operator never sees the
        # counter and never mutates the State it is handed.
        self.state.update(state)
        return None

    @overrides.final
    def run_initialization(self, initialization_code: str) -> None:
        # Run the user's `initialization` to seed the loop variables, then keep
        # them as self.state. The namespace is passed as exec globals (not a
        # locals-only mapping) so a comprehension / generator expression /
        # lambda in the init resolves its free variables -- a locals-only
        # namespace raises NameError on otherwise-valid init code. exec injects
        # `__builtins__` into that globals dict, so drop it before it reaches
        # the persisted state (it is not JSON-serializable and would break the
        # State materialization on the back-edge). A user variable named
        # `table` is left in place so produce_state_on_finish flags the
        # collision rather than silently dropping it.
        namespace: dict = {}
        exec(initialization_code, namespace)
        namespace.pop("__builtins__", None)
        self.state = State(namespace)

    @overrides.final
    def eval_output(self, output_expr: str, table: Table) -> TableLike:
        return _eval_loop_expr(output_expr, self.state, table)

    @overrides.final
    def produce_state_on_finish(self, port: int) -> State:
        # Emit the user's loop variables plus the buffered input table for the
        # matching LoopEnd. The table rides as an Arrow IPC stream, not pickle
        # (see `table_to_ipc_bytes` in core.models.table for why). Reads the
        # buffer through `_buffered_table` so a rename of `TableOperator`
        # doesn't silently break this.
        # A user loop variable named `table` would be overwritten by the input
        # table below, so flag the collision instead of silently dropping it.
        if _TABLE_KEY in self.state:
            raise _reserved_name_error(_TABLE_KEY)
        produced = State(self.state)
        produced[_TABLE_KEY] = table_to_ipc_bytes(self._buffered_table(port))
        return produced


class LoopEndOperator(TableOperator):
    """Base class for the runtime side of a Loop End operator.

    The generator in ``LoopEndOpDesc.scala`` emits a thin
    ``ProcessLoopEndOperator(LoopEndOperator)`` subclass that wires the
    user-supplied ``update`` expression into ``process_state(...)`` (via
    ``run_update``) and the ``condition`` expression into ``condition()`` (via
    ``eval_condition``); all substantive logic lives here.

    ``process_table`` yields each input table through as-is; ``process_state``
    runs the user's ``update`` and persists only user variables back into
    ``self.state`` (keeping the decoded table on ``self._loop_table``);
    ``condition()`` decides whether ``MainLoop.complete()`` fires the back-edge.

    Subclass contract: the generated subclass overrides ``process_state()`` and
    ``condition()`` only; all other methods are ``@overrides.final``.
    ``self.state`` / ``self._loop_table`` start empty and are populated only by
    ``run_update`` on the matching-loop consume, so ``condition()`` returns
    ``False`` until that first consume -- a LoopEnd that never consumed a
    matching state (e.g. an inner LoopEnd that only forwarded outer-loop
    pass-through state) must not fire the back-edge. Reserved names: see
    ``_RESERVED_STATE_KEYS``.
    """

    def __init__(self):
        super().__init__()
        # MainLoop.complete() calls condition() on every LoopEnd, including one
        # that never consumed a matching state (an inner LoopEnd that only
        # forwarded outer-loop pass-through state, or a loop that completed
        # without a matching-branch consume). run_update is what populates
        # self.state / self._loop_table, so initialize them here to avoid
        # AttributeError; a None _loop_table means "nothing consumed yet" and
        # condition() short-circuits to False (see eval_condition).
        self.state: State = State()
        self._loop_table: Optional[Table] = None

    @overrides.final
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table

    @overrides.final
    def run_update(self, update_code: str, state: State) -> None:
        # Run the user's `update` in a throwaway namespace seeded with the
        # incoming loop variables and the input table, then persist the user
        # variables back into self.state. The table arrives as an Arrow IPC
        # stream, not pickle (see `table_to_ipc_bytes` in core.models.table
        # for why); the decoded table is kept on self._loop_table so
        # condition() can read it after the update.
        input_table = table_from_ipc_bytes(state[_TABLE_KEY])
        namespace = {**state, _TABLE_KEY: input_table}
        # Pass the namespace as exec globals (not a locals-only mapping) so a
        # comprehension / generator expression / lambda in the user's `update`
        # resolves its free variables -- a locals-only namespace raises
        # NameError on otherwise-valid update code. exec injects `__builtins__`
        # into that globals dict; drop it so it does not leak into self.state.
        exec(update_code, namespace)
        namespace.pop("__builtins__", None)
        # `table` is runtime-owned; a user `update` that rebinds (or deletes)
        # it (a loop variable named `table`) would be silently dropped by the
        # strip below, so flag the collision instead.
        if namespace.get(_TABLE_KEY) is not input_table:
            raise _reserved_name_error(_TABLE_KEY)
        self._loop_table = input_table
        self.state = _strip_reserved(namespace)

    @overrides.final
    def eval_condition(self, condition_expr: str) -> bool:
        # No matching state was consumed (run_update never ran, so _loop_table
        # is still None): the loop never iterated here, so do not continue.
        # Returning False also avoids evaluating the user's condition against
        # loop variables that don't exist yet (which would raise NameError).
        if self._loop_table is None:
            return False
        return _eval_loop_expr(condition_expr, self.state, self._loop_table)

    @abstractmethod
    def condition(self) -> bool:
        pass
