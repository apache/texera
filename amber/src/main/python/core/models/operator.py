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

import ast
import overrides
import textwrap
import types
import pandas
from functools import lru_cache
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterator, List, Mapping, Optional, Union, MutableMapping, Protocol

from . import Table, TableLike, Tuple, TupleLike, Batch, BatchLike
from .state import State
from .table import all_output_to_tuple

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

    # The state message most recently handed to this operator -- inside a
    # control block, the iteration's loop variables. The runtime registers it
    # right before ``process_state`` runs (see ``DataProcessor.process_state``),
    # so the callback and every call after it can consult it. A class-level
    # default rather than an ``__init__`` assignment: a subclass ``__init__``
    # that skips ``super()`` still reads ``None`` until a state arrives. The
    # loop operators keep their own loop variables apart, on ``variables``.
    state: Optional[State] = None

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
            raise ValueError(f"BATCH_SIZE cannot be {type(value)}.")
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

    def _collected_table(self, port: int) -> Table:
        """The table collected on ``port`` so far."""
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
# user's value. The table itself never rides the State content: the LoopEnd
# runtime reads it from the Loop Start's input-port materialization at consume
# time and injects it via ``attach_loop_table``. The envelope names
# (``loop_counter`` / ``loop_start_id``) never enter user state -- they ride
# the StateFrame envelope (see ``core.models.payload``). The loop bookkeeping
# base URI is setup config, not state (see ``loopStartPortUris`` on the
# ``InitializeExecutorRequest`` proto).
_TABLE_KEY = "table"
# The paper calls the loop's input data D; the primitives may use either name.
_D_KEY = "D"
_RESERVED_STATE_KEYS: frozenset = frozenset({_TABLE_KEY, _D_KEY})


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


# What a cell may define besides its variables: helpers, which no State could
# serialize and no later primitive needs as a value.
_HELPER_TYPES = (
    types.ModuleType,
    types.FunctionType,
    types.BuiltinFunctionType,
    types.MethodType,
    type,
)


def _user_variables(namespace: dict) -> State:
    """The control variables a primitive's namespace holds: every name it bound,
    without the reserved ``table`` / ``D``, Python's own ``__dunder__`` names
    (``__builtins__``, ``__annotations__``) and helpers such as an ``import``ed
    module, a ``def`` or a ``class``."""
    return _strip_reserved(
        State(
            {
                key: value
                for key, value in namespace.items()
                if not (key.startswith("__") and key.endswith("__"))
                and not isinstance(value, _HELPER_TYPES)
            }
        )
    )


def _check_reserved_kept(namespace: dict, table: Optional[Table]) -> None:
    """Raise if user code rebound (or deleted) ``table`` or ``D`` in ``namespace``.

    Both are the runtime's input table; stripping a rebound one would silently
    drop the user's value, so flag the collision instead.
    """
    for key in _RESERVED_STATE_KEYS:
        if namespace.get(key) is not table:
            raise _reserved_name_error(key)


def _compile_cell(code: str, filename: str):
    """Compile a cell into its statements and its trailing expression.

    As in a notebook cell, a last line that is a bare expression is the cell's
    value. Returns ``(statements, expression)``; ``expression`` is ``None`` when
    the cell ends in a statement or is empty. A single expression, such as
    ``table.iloc[[i]]`` or ``None``, is a cell with no statements before it.
    """
    # eval() ignored the leading whitespace of the one-line Outputs written
    # before Output was a cell; dedent keeps those valid.
    module = ast.parse(textwrap.dedent(code), filename, "exec")
    expression = None
    if module.body and isinstance(module.body[-1], ast.Expr):
        last = module.body.pop()
        expression = compile(ast.Expression(body=last.value), filename, "eval")
    return compile(module, filename, "exec"), expression


def _eval_loop_expr(expr: str, state: State, table: Optional[Table]):
    """Evaluate ``expr`` directly against the loop variables plus ``table``.

    Runs in a throwaway namespace seeded with the loop variables and ``table``
    so the seeded ``table`` neither leaks into nor is clobbered out of the
    persistent loop ``state``. Used by LoopEnd's ``condition``; LoopStart's
    ``output`` is a cell (see ``LoopStartOperator._run_output_cell``).

    The namespace is passed as ``eval`` globals (not a locals-only mapping):
    a generator expression / comprehension / lambda in the user's expression
    resolves its free variables against globals, so a locals-only namespace
    would raise ``NameError`` on otherwise-valid expressions like
    ``all(x > threshold for x in items)``. The namespace is discarded, so the
    ``__builtins__`` that ``eval`` injects into it does not matter here.
    """
    namespace = {**state, _TABLE_KEY: table, _D_KEY: table}
    return eval(expr, namespace)


class LoopStartOperator(TableOperator):
    """Base class for the runtime side of a Loop Start operator.

    The generator in ``LoopStartOpDesc.scala`` emits a thin
    ``ProcessLoopStartOperator(LoopStartOperator)`` subclass that hands the
    user-supplied ``initialization`` and ``output`` cells over in ``open()``
    and yields the output cell's data in ``process_table()``; all substantive
    logic lives here.

    Once the input data D is complete, ``initialization`` seeds
    ``self.variables``, the values ``process_state`` merged in (the back edge)
    replace them, and the ``output`` cell sets this iteration's own;
    ``produce_state_on_finish`` emits those variables to the matching LoopEnd.
    The input table does NOT ride the state: the LoopEnd runtime reads it from
    this operator's input-port materialization at consume time
    (``loopStartPortUris``).
    ``loop_counter`` and the nested pass-through are owned by
    ``MainLoop._process_state_frame``, not this operator.

    Subclass contract: the generated subclass overrides ``open()`` and
    ``process_table()`` only; all other methods are ``@overrides.final``. After
    ``open()`` returns ``self.variables`` holds only the user's loop variables --
    not the reserved ``table``; see the ``_RESERVED_STATE_KEYS`` module comment
    for the ``table`` vs envelope-borne counter/id split.
    """

    # The output cell handed over in open() and the data it left for the body.
    # Class-level defaults: a subclass that never calls run_output has an
    # empty cell, which sets nothing and sends no data.
    _output_code: str = ""
    _output_data: Optional[TableLike] = None

    @overrides.final
    def process_state(self, state: State, port: int) -> Optional[State]:
        # First-entry only: merge upstream state into self.variables. The nested
        # pass-through (a frame already stamped with a LoopStartId) and all
        # loop_counter bookkeeping are owned by the worker runtime
        # (main_loop._process_state_frame), so this operator never sees the
        # counter and never mutates the State it is handed.
        self.variables.update(state)
        return None

    @overrides.final
    def run_initialization(self, initialization_code: str) -> None:
        # init() reads the input data D, which has not arrived when open() runs, so it is
        # kept and run once D is complete (see _initialize).
        self._initialization_code = initialization_code
        self._initialized = False
        self.variables = State()

    @overrides.final
    def run_output(self, output_code: str) -> None:
        # The output cell reads D too, so it is kept and run once D is complete,
        # right after init() and the back-edge values (see _initialize). It is
        # handed over here rather than in process_table: what it assigns are
        # control variables, and the runtime produces the state BEFORE it
        # drains on_finish, so the cell must be known by then.
        self._output_code = output_code

    def _initialize(self, table: Table) -> None:
        # Runs once, whichever of produce_state_on_finish / eval_output comes
        # first: init(), then the back-edge values, then the output cell.
        # Run the user's `initialization` to seed the loop variables, then keep
        # them as self.variables. The namespace is passed as exec globals (not a
        # locals-only mapping) so a comprehension / generator expression /
        # lambda in the init resolves its free variables -- a locals-only
        # namespace raises NameError on otherwise-valid init code. exec injects
        # `__builtins__` into that globals dict, so drop it before it reaches
        # the persisted state (it is not JSON-serializable and would break the
        # State materialization on the back-edge). A user variable named
        # `table` is left in place so produce_state_on_finish flags the
        # collision rather than silently dropping it.
        if self._initialized:
            return
        self._initialized = True
        namespace: dict = {_TABLE_KEY: table, _D_KEY: table}
        exec(self._initialization_code, namespace)
        _check_reserved_kept(namespace, table)
        variables = _user_variables(namespace)
        # A value that arrived before, on the back edge, replaces the one init() set.
        variables.update(self.variables)
        self.variables = State(variables)
        self._output_data = self._run_output_cell(table)

    def _run_output_cell(self, table: Table) -> Optional[TableLike]:
        # Every name the output cell assigns becomes a control variable of this
        # iteration, replacing one from init() or the back edge; the value of a
        # trailing bare expression is the data sent to the body, and without
        # one the body gets no data, only the state. Statements and expression
        # run against one namespace, passed as globals (as in _initialize) so a
        # lambda / generator expression resolves its free names.
        statements, expression = _compile_cell(self._output_code, "<output>")
        namespace: dict = {**self.variables, _TABLE_KEY: table, _D_KEY: table}
        exec(statements, namespace)
        # Checked before the expression runs, so a rebound D is reported as
        # such rather than as whatever the expression then does with it...
        _check_reserved_kept(namespace, table)
        data = None
        if expression is not None:
            # The trailing expression runs on a copy: as when Output was a
            # single eval()ed expression, a walrus in it binds no variable.
            scope = dict(namespace)
            data = eval(expression, scope)
            # ...and after, for a `(D := ...)` in the expression itself.
            _check_reserved_kept(scope, table)
        # Merged rather than replaced, so a `table` that arrived on the back
        # edge is still there for produce_state_on_finish to flag.
        self.variables.update(_user_variables(namespace))
        return data

    @overrides.final
    def eval_output(self, table: Table) -> Optional[TableLike]:
        # The data the output cell left. The cell normally ran already, when
        # the state was produced; if on_finish comes first it runs here, once.
        # The data may be None: then the body gets no data, only the state.
        self._initialize(table)
        return self._output_data

    @overrides.final
    def produce_state_on_finish(self, port: int) -> State:
        # Emit the user's loop variables for the matching LoopEnd. The input
        # table does NOT ride the state: the LoopEnd runtime reads it from
        # this operator's input-port materialization at consume time
        # (loopStartPortUris). A user loop variable named `table` would be
        # silently shadowed by that injected table in the LoopEnd's
        # update/condition namespaces, so flag the collision here.
        # _initialize also runs the output cell, so what it assigns is in the
        # state; on_finish, which drains its data, runs after this.
        self._initialize(self._collected_table(port))
        for key in _RESERVED_STATE_KEYS:
            if key in self.variables:
                raise _reserved_name_error(key)
        return State(self.variables)


class LoopEndOperator(TableOperator):
    """Base class for the runtime side of a Loop End operator.

    The generator in ``LoopEndOpDesc.scala`` emits a thin
    ``ProcessLoopEndOperator(LoopEndOperator)`` subclass that wires the
    user-supplied ``update`` expression into ``process_state(...)`` (via
    ``run_update``) and the ``condition`` expression into ``condition()`` (via
    ``eval_condition``); all substantive logic lives here.

    ``process_table`` yields each input table through as-is; ``process_state``
    runs the user's ``update`` (against the table the runtime attached via
    ``attach_loop_table``) and persists only user variables back into
    ``self.variables``; ``condition()`` decides whether ``MainLoop.complete()``
    fires the back-edge.

    Subclass contract: the generated subclass overrides ``process_state()`` and
    ``condition()`` only; all other methods are ``@overrides.final``.
    ``self.variables`` / ``self._loop_table`` start empty and are populated only by
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
        # self.variables / self._loop_table, so initialize them here to avoid
        # AttributeError; a None _loop_table means "nothing consumed yet" and
        # condition() short-circuits to False (see eval_condition).
        self.variables: State = State()
        # Set by the runtime (attach_loop_table) right before the matching
        # consume and taken (cleared) by run_update. The clear is defensive
        # rather than load-bearing today -- a worker instance handles a single
        # iteration (workers are recreated per region execution), so there is
        # no second consume within one instance for a stale table to leak
        # into -- but nothing may run an update against a table it was not
        # explicitly handed. It stays separate from _loop_table because
        # _loop_table doubles as the "consumed" marker that condition()
        # short-circuits on, and only a SUCCESSFUL update may set that.
        self._attached_table: Optional[Table] = None
        self._loop_table: Optional[Table] = None

    @overrides.final
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table

    @overrides.final
    def attach_loop_table(self, table: Table) -> None:
        # Runtime-only hook: MainLoop reads the loop's input table from the
        # Loop Start's input-port materialization (loopStartPortUris) and
        # attaches it here right before the matching consume, so the table
        # never has to ride inside the State content through the loop body.
        self._attached_table = table

    @overrides.final
    def run_update(self, update_code: str, state: State) -> None:
        # Run the user's `update` in a throwaway namespace seeded with the
        # incoming loop variables and the input table, then persist the user
        # variables back into self.variables. The table is attached by the runtime
        # (attach_loop_table) from the Loop Start's input materialization; on
        # a successful update it is kept on self._loop_table so condition()
        # can read it afterwards.
        if self._attached_table is None:
            raise RuntimeError(
                "loop input table was not attached before the update; the "
                "runtime must call attach_loop_table on the matching consume"
            )
        input_table = self._attached_table
        # Take it: a later iteration that never got a table must raise above
        # rather than silently run the update against the previous one.
        self._attached_table = None
        namespace = {**state, _TABLE_KEY: input_table, _D_KEY: input_table}
        # Pass the namespace as exec globals (not a locals-only mapping) so a
        # comprehension / generator expression / lambda in the user's `update`
        # resolves its free variables -- a locals-only namespace raises
        # NameError on otherwise-valid update code. exec injects `__builtins__`
        # into that globals dict; drop it so it does not leak into self.variables.
        exec(update_code, namespace)
        # `table` is runtime-owned; a user `update` that rebinds (or deletes)
        # it (a loop variable named `table`) would be silently dropped by the
        # strip below, so flag the collision instead.
        for key in _RESERVED_STATE_KEYS:
            if namespace.get(key) is not input_table:
                raise _reserved_name_error(key)
        self._loop_table = input_table
        self.variables = _user_variables(namespace)

    @overrides.final
    def eval_condition(self, condition_expr: str) -> bool:
        # No matching state was consumed (run_update never ran, so _loop_table
        # is still None): the loop never iterated here, so do not continue.
        # Returning False also avoids evaluating the user's condition against
        # loop variables that don't exist yet (which would raise NameError).
        if self._loop_table is None:
            return False
        return _eval_loop_expr(condition_expr, self.variables, self._loop_table)

    @abstractmethod
    def condition(self) -> bool:
        pass
