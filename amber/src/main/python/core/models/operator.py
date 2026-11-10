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

import math
import overrides
import pandas
import re
import unicodedata
from functools import lru_cache
from abc import ABC, abstractmethod
from collections import defaultdict
from decimal import Decimal
from typing import (
    Any,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
    MutableMapping,
    Protocol,
    Tuple as TypingTuple,
)

from . import Table, TableLike, Tuple, TupleLike, Batch, BatchLike, Schema
from .state import State
from .table import all_output_to_tuple

import base64


def require_yielded(result, method_name: str):
    """
    Catch the common case of a user method such as process_table using
    `return` instead of `yield`: it rejects a returned None, Tuple, dict or
    DataFrame, which would otherwise fail later with a confusing error.
    Other values (e.g. a list, or a pandas Series) are passed through
    unchecked.
    """
    if result is None or isinstance(result, (Tuple, dict, pandas.DataFrame)):
        raise TypeError(
            f"{method_name} must `yield` results, not `return` them. "
            "Replace `return x` with `yield x`."
        )
    return result


class _LoopVariables(Mapping):
    """
    A read-only view of an operator's loop variables. Reading a name that got
    two different values from the same loop raises the error a JVM operator
    whose setting refers to that name raises (``StateReferenceBinding``); the
    other names read as usual, and the name itself is still listed.
    """

    def __init__(
        self,
        values: Mapping[str, Any],
        conflicts: Mapping[str, TypingTuple[Any, Any]],
    ) -> None:
        self._values = values
        self._conflicts = conflicts

    def __getitem__(self, name: str) -> Any:
        if name in self._conflicts:
            earlier, later = self._conflicts[name]
            raise RuntimeError(
                f"loop variable {name} got two different values in one iteration, "
                f"{earlier} and {later}: a loop's variables must not change inside "
                "its body"
            )
        return self._values[name]

    def __contains__(self, name: object) -> bool:
        return name in self._values

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


def _java_double_text(value: float) -> str:
    """
    ``value`` as Java's ``Double.toString`` spells it, which is how a JVM
    operator's text property binds a loop variable the JVM reads as a double:
    the fewest digits that read back as ``value`` (Python's ``repr`` finds
    them), in plain notation from 10^-3 up to 10^7 and as ``d.ddd`` with an
    ``E`` exponent outside that, always with a digit after the point; and
    ``NaN``, ``Infinity`` and ``-Infinity``. JDK 17 prints a few doubles with
    other digits (JDK-4511638, fixed in JDK 19), mostly beyond 10^16 or
    below 10^-7; its text reads back as the same double.
    """
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    negative, digit_tuple, exponent = Decimal(repr(value)).as_tuple()
    digits = "".join(map(str, digit_tuple)).rstrip("0") or "0"
    # value == 0.<digits> * 10**point
    point = len(digit_tuple) + exponent
    if -3 < point < 8:
        if point <= 0:
            text = "0." + "0" * -point + digits
        else:
            text = digits[:point].ljust(point, "0") + "." + (digits[point:] or "0")
    else:
        text = digits[0] + "." + (digits[1:] or "0") + "E" + str(point - 1)
    return "-" + text if negative else text


def _loop_variable_text(name: str, variables: Mapping[str, Any], bound: bool) -> str:
    """
    Loop variable ``name`` of ``variables`` as ``Operator.loop_variable_text``
    reads it; ``bound`` tells whether ``variables`` are the values the
    operator bound, which the error for a missing name says.
    """
    if name not in variables:
        raise RuntimeError(
            f"the operator refers to loop variable ${name}, "
            "but no state message carried it"
            + (
                " before the operator's first tuple or the end of an input"
                if bound
                else ""
            )
        )
    value = variables[name]
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not -(2**63) <= value < 2**63:
        raise RuntimeError(
            f"the operator refers to loop variable ${name}, but its value {value} "
            "does not fit in the 64-bit integer a JVM operator reads it as"
        )
    if isinstance(value, float):
        return _java_double_text(value)
    if isinstance(value, (str, int)):
        return str(value)
    raise RuntimeError(
        f"the operator refers to loop variable ${name}, "
        f"but its value {value!r} is not a scalar"
    )


# The call through which the code generated for an operator inside a control
# block reads a loop variable one of its text properties refers to, as the
# compiler writes it (PythonOperatorDescriptor.loopVariableLookup): the name of
# a `$name` reference, without the `$`.
_LOOP_VARIABLE_TEXT_LOOKUP = re.compile(
    r"self\.loop_variable_text\('([A-Za-z_][A-Za-z0-9_]*)'\)"
)


# The same for a numeric or boolean property's reference, its kind beside it
# (PythonOperatorDescriptor.loopVariableValueLookup).
_LOOP_VARIABLE_VALUE_LOOKUP = re.compile(
    r"self\.loop_variable_value\('([A-Za-z_][A-Za-z0-9_]*)', '([a-z]+)'\)"
)


def referenced_loop_variables(code: str) -> List[str]:
    """
    The loop variables ``code`` reads through ``Operator.loop_variable_text``,
    each once, in name order: in the code generated for an operator inside a
    control block, the ones its properties refer to.
    """
    return sorted(set(_LOOP_VARIABLE_TEXT_LOOKUP.findall(code)))


def referenced_loop_variable_values(code: str) -> List[tuple]:
    """
    The loop variables ``code`` reads through ``Operator.loop_variable_value``,
    each as its name and kind, once, in that order: in the code generated for
    an operator inside a control block, the ones its numeric and boolean
    properties refer to.
    """
    return sorted(set(_LOOP_VARIABLE_VALUE_LOOKUP.findall(code)))


# Each kind loop_variable_value reads, and what a value of it is called.
_VALUE_KINDS = {
    "integer": "an integer",
    "long": "an integer",
    "number": "a number",
    "boolean": "a boolean",
}


def _check_value_kind(name: str, kind: str) -> None:
    """Refuse a kind the generated code never asks ``loop_variable_value`` for."""
    if kind not in _VALUE_KINDS:
        raise ValueError(
            f"loop variable ${name} cannot be read as {kind!r}: "
            "the kind is one of 'integer', 'long', 'number' and 'boolean'"
        )


def _loop_variable_value(
    name: str, kind: str, variables: Mapping[str, Any], bound: bool
) -> Union[int, float, bool]:
    """
    Loop variable ``name`` of ``variables`` as ``Operator.loop_variable_value``
    reads it as a ``kind``; ``bound`` as for ``_loop_variable_text``.
    """
    _check_value_kind(name, kind)
    text = _loop_variable_text(name, variables, bound).strip(_JAVA_TRIMMED)
    if kind in ("integer", "long"):
        value = _parse_java_long(text)
    elif kind == "number":
        value = _parse_java_double(text)
    else:
        # Scala's toBooleanOption: equalsIgnoreCase, which also takes the
        # long s (ſ) for an s, as its upper case is S.
        value = {"TRUE": True, "FALSE": False}.get(text.upper())
    if value is None:
        raise RuntimeError(
            f"the operator refers to loop variable ${name}, "
            f"but its value {variables[name]!r} is not {_VALUE_KINDS[kind]}"
        )
    if kind == "integer" and not -(2**31) <= value < 2**31:
        raise RuntimeError(
            f"the operator refers to loop variable ${name}, "
            f"but its value {variables[name]!r} does not fit its property, "
            "a 32-bit integer"
        )
    return value


# What Java's String.trim drops from either end of a text: every character from
# U+0000 to U+0020, the space and the control characters below it, and nothing
# above it.
_JAVA_TRIMMED = "".join(map(chr, range(0x21)))

# The numerals Java's Double.parseDouble reads, besides NaN and Infinity: a
# decimal one, or a hexadecimal one with a binary exponent, each with an
# optional sign and an f, F, d or D suffix that changes nothing. Their digits
# are ASCII ones.
_JAVA_DECIMAL_DOUBLE = re.compile(
    r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[fFdD]?"
)
_JAVA_HEX_DOUBLE = re.compile(
    r"[+-]?0[xX](?:[0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?"
)

# The numerals Java's new BigDecimal(String) reads, their digits as ASCII ones:
# a sign, digits with at most one point, and an exponent; groups: the sign, the
# digits before the point, the digits after it, the exponent.
_JAVA_BIG_DECIMAL = re.compile(r"([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?")


def _parse_java_double(text: str) -> Optional[float]:
    """
    ``text`` read as Java's ``Double.parseDouble`` reads a trimmed text, or
    ``None`` where that throws. Python's ``float`` reads more (``inf``,
    ``nan``, ``1_000``, other scripts' digits) and less (``2.5d``,
    ``0x1p3``).
    """
    unsigned = text[1:] if text[:1] in ("+", "-") else text
    if unsigned == "NaN":
        return math.nan
    if unsigned == "Infinity":
        return -math.inf if text[0] == "-" else math.inf
    if _JAVA_DECIMAL_DOUBLE.fullmatch(text):
        return float(text.rstrip("fFdD"))
    if _JAVA_HEX_DOUBLE.fullmatch(text):
        try:
            return float.fromhex(text.rstrip("fFdD"))
        except OverflowError:  # Java rounds it to an infinity
            return -math.inf if text[0] == "-" else math.inf
    return None


def _parse_java_long(text: str) -> Optional[int]:
    """
    ``text`` read as Java's ``new BigDecimal(text).longValueExact()`` reads a
    trimmed text, or ``None`` where that throws: a numeral that is a whole
    number within a signed 64-bit long, such as ``42``, ``4.2e1`` or ``3.0``.
    Its digits may be any script's, but only of the Basic Multilingual Plane,
    as Java reads a text one UTF-16 unit at a time, and its exponent and
    scale must each fit a 32-bit int. Python's ``Decimal`` reads more
    (``inf``, ``nan``, ``1_000``, an exponent of any size).
    """
    if any(ord(char) > 0xFFFF for char in text):
        return None
    ascii_text = "".join(
        str(unicodedata.decimal(char)) if char.isdecimal() else char for char in text
    )
    match = _JAVA_BIG_DECIMAL.fullmatch(ascii_text)
    if match is None or not (match[2] or match[3]):
        return None
    sign, whole, fraction, exponent = match.groups(default="")
    exponent_digits = exponent.lstrip("+-").lstrip("0")
    if len(exponent_digits) > 10:
        return None
    power = int(exponent_digits or "0") * (-1 if exponent[:1] == "-" else 1)
    scale = len(fraction) - power
    if not -(2**31) <= power < 2**31 or not -(2**31) <= scale < 2**31:
        return None
    digits = (whole + fraction).lstrip("0")
    if not digits:
        return 0
    # The value is significant * 10**shift, a whole number when shift >= 0.
    significant = digits.rstrip("0")
    shift = len(digits) - len(significant) - scale
    if shift < 0 or len(significant) + shift > 19:
        return None
    value = int(sign + significant) * 10**shift
    return value if -(2**63) <= value < 2**63 else None


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

    def loop_variable_text(self, name: str) -> str:
        """
        The text of loop variable ``name`` as ``loop_variables`` holds it (see
        ``register_state`` for which value wins, and for the error a name that
        got two different values raises). Inside a control block, the
        code generated for an operator calls this where a text property holds
        ``$name``, in place of decoding the literal, so the value is read when
        the code runs. It is spelled as a JVM operator's text property binds it
        (``StateReferenceBinding``): a boolean as ``true`` or ``false``, and
        only a scalar at all. From the operator's first tuple on, it reads the
        values bound then instead (``bind_loop_variables``). A float is spelled
        as Java's ``Double.toString`` spells it (``_java_double_text``), and an
        integer past 64 bits, which a JVM operator reads wrapped, is refused.

        :param name: str, the loop variable's name, without the leading ``$``.
        :return: str, the variable's value, as text.
        """
        bound = self.__internal_bound_loop_variables
        if bound is None and self.__internal_loop_variables is None:
            raise RuntimeError(
                f"loop variable ${name} is read before the iteration's state arrived"
            )
        if bound is None:
            return _loop_variable_text(name, self.loop_variables, False)
        return _loop_variable_text(name, bound, True)

    def loop_variable_value(self, name: str, kind: str) -> Union[int, float, bool]:
        """
        The value of loop variable ``name`` as a ``kind``. Inside a control
        block, the code generated for an operator calls this where a numeric or
        boolean property holds ``$name``, in place of the value written into
        the code, so the value is read when the code runs. It is read from the
        variable's text (``loop_variable_text``, so the values bound at the
        operator's first tuple), as a JVM operator's numeric or boolean
        property binds it (``StateReferenceBinding``): the text trimmed as
        Java's ``String.trim`` trims it, then an integer as
        ``new BigDecimal(text).longValueExact()`` reads it (a whole number
        within a signed 64-bit long, and within 32 bits for an int property,
        as Jackson refuses a larger one there), a number as
        ``Double.parseDouble`` does, and a boolean as ``true`` or ``false`` in
        any case, never ``1`` or ``0``.

        :param name: str, the loop variable's name, without the leading ``$``.
        :param kind: str, the property that holds ``$name``: ``integer`` (a
            Java ``int`` or ``Integer``), ``long`` (a ``long`` or ``Long``),
            ``number`` or ``boolean``.
        :return: int, float or bool, the variable's value as ``kind``.
        """
        _check_value_kind(name, kind)
        bound = self.__internal_bound_loop_variables
        if bound is None and self.__internal_loop_variables is None:
            raise RuntimeError(
                f"loop variable ${name} is read before the iteration's state arrived"
            )
        if bound is None:
            return _loop_variable_value(name, kind, self.loop_variables, False)
        return _loop_variable_value(name, kind, bound, True)

    __internal_is_source: bool = False

    # The loop variables received so far, by name, and the nesting depth of
    # the message each value came in; see ``register_state``. ``None`` until
    # the first state message. Class-level defaults rather than ``__init__``
    # assignments: a subclass ``__init__`` that skips ``super()`` still reads
    # no loop variables until a state arrives.
    __internal_loop_variables: Optional[MutableMapping[str, Any]] = None
    __internal_loop_variable_depths: Optional[MutableMapping[str, int]] = None
    # The names that got two different values at the depth they are held at,
    # with those two values; see ``register_state``.
    __internal_loop_variable_conflicts: Optional[
        MutableMapping[str, TypingTuple[Any, Any]]
    ] = None
    # The loop variables as they were when the operator first saw data; see
    # ``bind_loop_variables``. ``None`` until then.
    __internal_bound_loop_variables: Optional[Mapping[str, Any]] = None
    # The loop variables the operator's properties refer to, which binding
    # requires; see ``require_loop_variables``.
    __internal_required_loop_variables: Sequence[str] = ()
    # The same for its numeric and boolean properties, each with its kind; see
    # ``require_loop_variable_values``.
    __internal_required_loop_variable_values: Sequence[tuple] = ()

    @property
    @overrides.final
    def loop_variables(self) -> Mapping[str, Any]:
        """
        The loop variables received from upstream, read-only: inside a control
        block, the values the state messages handed to this operator carried,
        by name. The runtime registers each message (``register_state``) right
        before ``process_state`` runs, so the callback and every call after it
        can read them. Empty until a state message arrives. Reading a name
        that got two different values from the same loop raises an error; see
        ``register_state``.
        """
        return _LoopVariables(
            self.__internal_loop_variables or {},
            self.__internal_loop_variable_conflicts or {},
        )

    @overrides.final
    def register_state(self, state: State, loop_counter: int = 0) -> None:
        """
        Merge the values ``state`` carries into ``loop_variables``. The runtime
        calls this for every state message, with the nesting depth its envelope
        carries (``loop_counter``): 0 from the innermost loop around this
        operator, 1 from the loop around that, and so on. A state no loop sent,
        such as one an operator returns from ``produce_state_on_finish``,
        travels at depth 0.

        For each name, the value from the smallest depth wins. So an inner
        loop's variable shadows an enclosing one of the same name, even when the
        enclosing message arrives later, and a state adds only the names it
        carries. A message from the same depth may repeat a value, as copies
        from several upstream workers or branches do, but must not change it:
        reading a name that got two different values from one depth raises the
        error a JVM operator whose setting refers to it raises
        (``StateReferenceBinding``), until a value from a smaller depth replaces
        both.

        :param state: State, the state message that arrived.
        :param loop_counter: int, the message's nesting depth.
        """
        if self.__internal_loop_variables is None:
            self.__internal_loop_variables = {}
            self.__internal_loop_variable_depths = {}
            self.__internal_loop_variable_conflicts = {}
        for name, value in state.items():
            depth = self.__internal_loop_variable_depths.get(name)
            if depth is None or loop_counter < depth:
                self.__internal_loop_variables[name] = value
                self.__internal_loop_variable_depths[name] = loop_counter
                self.__internal_loop_variable_conflicts.pop(name, None)
            elif loop_counter == depth:
                earlier = self.__internal_loop_variables[name]
                if earlier != value:
                    self.__internal_loop_variable_conflicts.setdefault(
                        name, (earlier, value)
                    )

    @overrides.final
    def require_loop_variables(self, names: Iterable[str]) -> None:
        """
        Make binding (``bind_loop_variables``) require the loop variables
        ``names``: the ones the operator's properties refer to, which its
        generated code reads through ``loop_variable_text``. The runtime calls
        this when it builds the operator from its code, with the names the code
        reads (``referenced_loop_variables``).

        :param names: Iterable[str], the loop variables' names, without ``$``.
        """
        self.__internal_required_loop_variables = tuple(names)

    @overrides.final
    def require_loop_variable_values(self, references: Iterable[tuple]) -> None:
        """
        As ``require_loop_variables``, for the operator's numeric and boolean
        properties, which its generated code reads through
        ``loop_variable_value``: each reference is the loop variable's name and
        the kind it is read as (``referenced_loop_variable_values``). Binding
        then also fails while one of them has no value its property can take.

        :param references: Iterable[tuple], each loop variable's name, without
            ``$``, and kind.
        """
        self.__internal_required_loop_variable_values = tuple(
            (name, kind) for name, kind in references
        )

    @overrides.final
    def bind_loop_variables(self) -> None:
        """
        Fix the loop variables the operator's own properties read
        (``loop_variable_text``) to ``loop_variables`` as it is now. The runtime
        calls this right before the operator's first tuple, on any input port,
        and when an input port finishes, in case none came (a source produces
        its output there); only the first call binds. So, as a JVM operator's
        properties (``bindStateReferences``), they hold one value for the whole
        iteration, while ``loop_variables`` still takes every later state
        message for UDF code to read. The next iteration runs on a fresh
        worker, which binds again. A name that got two different values is
        bound with both, so reading it fails there too.

        Binding fails, naming each one in one error, the texts first, while a
        loop variable the operator requires (``require_loop_variables``,
        ``require_loop_variable_values``) has no value its property can take:
        no state message carried it, it got two different values
        (``register_state``), or the value is not a scalar, not within 64
        bits, or not of the property's kind. As a JVM operator's binding does,
        it then binds nothing, whether or not the code would read the value,
        and a later binding tries again. A name with two values is named once,
        however many properties read it, as a JVM operator fails for it once.
        A name the operator does not require may have two values: only a read
        of it fails then, as a JVM operator fails only for a name its setting
        refers to.
        """
        if self.__internal_bound_loop_variables is not None:
            return
        conflicts = dict(self.__internal_loop_variable_conflicts or {})
        variables = _LoopVariables(
            dict(self.__internal_loop_variables or {}), conflicts
        )
        problems = []
        for name in self.__internal_required_loop_variables:
            try:
                _loop_variable_text(name, variables, True)
            except RuntimeError as error:
                problems.append(str(error))
        for name, kind in self.__internal_required_loop_variable_values:
            try:
                _loop_variable_value(name, kind, variables, True)
            except RuntimeError as error:
                # Every read of a name with two values fails with the same
                # error, which a text or an earlier kind may have named already.
                if name not in conflicts or str(error) not in problems:
                    problems.append(str(error))
        if problems:
            raise RuntimeError("; ".join(problems))
        self.__internal_bound_loop_variables = variables

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

    __internal_input_schemas: Optional[MutableMapping[int, Schema]] = None

    @property
    @overrides.final
    def input_schemas(self) -> MutableMapping[int, Schema]:
        """
        What each input port was declared to carry, keyed by port index and
        written by the runtime before that port's data is handed over.

        The tuples themselves say this too, so an operator only needs to ask
        when there are none: a port can finish having carried no rows at all,
        and its schema is then the only record of what its columns were.
        """
        if self.__internal_input_schemas is None:
            self.__internal_input_schemas = {}
        return self.__internal_input_schemas

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
        for i in require_yielded(self.produce(), "produce"):
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

    def _get_batch_data(self) -> MutableMapping[int, List[Tuple]]:
        # Created on first use (not in __init__) so a subclass whose __init__
        # skips super().__init__() still works, like _get_template_decoder.
        if not hasattr(self, "_BatchOperator__batch_data"):
            self._validate_batch_size(self.BATCH_SIZE)
            self.__batch_data = defaultdict(list)
        return self.__batch_data

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        batch_data = self._get_batch_data()
        batch_data[port].append(tuple_)
        if self.BATCH_SIZE is not None and len(batch_data[port]) >= self.BATCH_SIZE:
            yield from self._process_batch(port)

    @overrides.final
    def _process_batch(self, port: int) -> Iterator[Optional[BatchLike]]:
        batch_data = self._get_batch_data()
        batch = Batch(
            pandas.DataFrame(
                [
                    batch_data[port].pop(0).as_series()
                    for _ in range(min(len(batch_data[port]), self.BATCH_SIZE))
                ]
            )
        )
        for output_batch in require_yielded(
            self.process_batch(batch, port), "process_batch"
        ):
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
        while len(self._get_batch_data()[port]) != 0:
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

    def _get_table_data(self) -> Mapping[int, List[Tuple]]:
        # Created on first use (not in __init__) so a subclass whose __init__
        # skips super().__init__() still works, like _get_template_decoder.
        if not hasattr(self, "_TableOperator__table_data"):
            self.__table_data = defaultdict(list)
        return self.__table_data

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        self._get_table_data()[port].append(tuple_)
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TableLike]]:
        rows = self._get_table_data()[port]
        schema = self.input_schemas.get(port)
        # A port that carried no rows has no tuples to read column names off,
        # and a table of no columns fails every operator that names one.
        table = Table(rows) if rows or schema is None else Table.empty_of(schema)
        yield from require_yielded(self.process_table(table, port), "process_table")

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
    emits those variables to the matching LoopEnd. The input table does NOT
    ride the state: the LoopEnd runtime reads it from this operator's
    input-port materialization at consume time (``loopStartPortUris``).
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
        # Emit the user's loop variables for the matching LoopEnd. The input
        # table does NOT ride the state: the LoopEnd runtime reads it from
        # this operator's input-port materialization at consume time
        # (loopStartPortUris). A user loop variable named `table` would be
        # silently shadowed by that injected table in the LoopEnd's
        # update/condition namespaces, so flag the collision here.
        if _TABLE_KEY in self.state:
            raise _reserved_name_error(_TABLE_KEY)
        return State(self.state)


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
    ``self.state``; ``condition()`` decides whether ``MainLoop.complete()``
    fires the back-edge.

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
        # variables back into self.state. The table is attached by the runtime
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
