/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator

/** Python definitions shared by several operators' standalone code, emitted
  * once per script via [[StandaloneCodeGenerator.standaloneHelpers]].
  */
object StandaloneHelpers {

  /**
    * A Python transcription of `AttributeTypeUtils`, for operators that cast a
    * column to a declared type.
    *
    * Python's own conversions answer differently on the values a spreadsheet
    * column actually holds. `bool("false")` is true, because every non-empty
    * string is, and `int("6.7")` raises where the engine reads 6: a cast goes
    * through `parseField(force = true)`, whose numeric branch is
    * `java.text.NumberFormat`, which truncates a decimal, drops a grouping
    * comma and stops at trailing letters. The engine reads "false" as false and
    * "0" as false too, so the script has to do the same rather than hand back a
    * column the workflow never produced.
    *
    * Refusing is still part of the contract where the engine refuses: text with
    * no leading digits raises, and a script that quietly wrote NaN instead
    * would report an answer the run it was exported from never reached.
    *
    *  - `_texera_java_trim`: `String.trim`, dropping only the characters up to U+0020.
    *  - `_texera_parse_int`: `Integer.parseInt` or `Long.parseLong`, a sign and digits in range.
    *  - `_texera_java_double`: `Double.parseDouble`, with a d/f suffix and hex, refusing the rest.
    *  - `_texera_double_key`: `Double.compareTo`'s order, -0.0 below 0.0 and NaN above all.
    *  - `_texera_key_codes`: one code per row's key under Scala's ==, shared across the frames
    *    given, where a null meets a null, 0.0 meets -0.0 and each NaN stands alone. With
    *    `signed`, a double is keyed with its sign, as the set operators' `HashSet[Tuple]`
    *    tells 0.0 from -0.0 and a double from an integer.
    *  - `_texera_read_doubles`: a DOUBLE column pandas left as text, read the engine's way.
    */
  val AttributeCasts: String =
    """# AttributeTypeUtils, transcribed so a cast answers as the engine does.
      |def _texera_cast_boolean(x):
      |    # toBoolean first, then `toInt == 1`: "0" and "2" are both false.
      |    if isinstance(x, str):
      |        text = _texera_java_trim(x)
      |        lowered = text.lower()
      |        if lowered == "true":
      |            return True
      |        if lowered == "false":
      |            return False
      |        return _texera_parse_int(text, 32) == 1
      |    return x != 0
      |
      |def _texera_parse_number(text):
      |    # java.text.NumberFormat for Locale.US, which a cast reaches through
      |    # `parseField(force = true)`. Lenient where Python's int is not: it stops
      |    # at the first character that cannot continue a number ("12abc" is 12),
      |    # drops "," without checking group sizes ("1,23" is 123), and reads "."
      |    # as a decimal point. It refuses a leading "+" and text with no digits.
      |    #
      |    # Also returns whether it came back as a Double, which decides between
      |    # the two narrowings below.
      |    import re
      |
      |    match = re.match(r"\s*(-?)([0-9,]*)(?:\.([0-9]*))?", text)
      |    sign = -1 if match.group(1) == "-" else 1
      |    digits = (match.group(2) or "").replace(",", "")
      |    fraction = match.group(3) or ""
      |    if not digits and not fraction:
      |        raise ValueError("Unparseable number: " + repr(text))
      |    if fraction:
      |        return sign * float((digits or "0") + "." + fraction), True
      |    value = sign * int(digits)
      |    # A whole number past long's range comes back as a Double.
      |    if -(2 ** 63) <= value <= 2 ** 63 - 1:
      |        return value, False
      |    return float(value), True
      |
      |def _texera_long_value(value, is_double):
      |    # Number.longValue(): a Double truncates toward zero and saturates at
      |    # long's bounds, where a Long is already itself.
      |    if is_double:
      |        return max(-(2 ** 63), min(2 ** 63 - 1, int(value)))
      |    return value
      |
      |def _texera_cast_integral(x):
      |    # The LONG target. Scala's toLong takes no decimal point of its own, so
      |    # a fraction only ever arrives already parsed.
      |    if isinstance(x, str):
      |        return _texera_long_value(*_texera_parse_number(x))
      |    if isinstance(x, bool):
      |        return 1 if x else 0
      |    if isinstance(x, float):
      |        return _texera_long_value(x, True)
      |    return int(x)
      |
      |def _texera_cast_int32(x, wrap):
      |    # The two 32-bit narrowings differ: Long.toInt keeps the low bits, so
      |    # 2147483648 comes back as -2147483648, while Double.toInt saturates.
      |    #
      |    # Text decides by what the parse returned; everything else is told by the
      |    # SOURCE column's declared type, because `Series.apply` hands the cells of
      |    # a nullable integer column over as floats.
      |    if isinstance(x, str):
      |        value, is_double = _texera_parse_number(x)
      |        wrap = not is_double
      |    elif isinstance(x, bool):
      |        value = 1 if x else 0
      |    else:
      |        value = int(x)
      |    if wrap:
      |        return ((value + 2147483648) % 4294967296) - 2147483648
      |    # int() first: Double.intValue truncates toward zero before it clamps.
      |    return max(-2147483648, min(2147483647, int(value)))
      |
      |def _texera_epoch_millis_to_timestamp(s):
      |    # `new Timestamp(long)` reads MILLISECONDS where pd.to_datetime defaults
      |    # to nanoseconds, and renders in the JVM's default zone, so leaving the
      |    # result in UTC would put it a whole offset away. Text needs neither: a
      |    # parsed wall clock is already the wall clock.
      |    # tzlocal() and not the current offset: the zone carries its daylight
      |    # rules, and each instant needs the offset in force when it happened.
      |    from dateutil.tz import tzlocal
      |
      |    return (
      |        pd.to_datetime(s, unit="ms", errors="coerce", utc=True)
      |        .dt.tz_convert(tzlocal())
      |        .dt.tz_localize(None)
      |    )
      |
      |def _texera_cast_double(x):
      |    return _texera_java_double(x) if isinstance(x, str) else float(x)
      |
      |def _texera_java_trim(text):
      |    return text.strip("".join(map(chr, range(33))))
      |
      |def _texera_parse_int(text, bits):
      |    import re
      |
      |    text = _texera_java_trim(text)
      |    bound = 2 ** (bits - 1)
      |    if not re.fullmatch(r"[+-]?\d+", text) or not -bound <= int(text) < bound:
      |        raise ValueError("For input string: " + repr(text))
      |    return int(text)
      |
      |def _texera_java_double(text):
      |    import re
      |
      |    text = _texera_java_trim(text)
      |    if not re.fullmatch(
      |        r"[+-]?(NaN|Infinity|((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?"
      |        r"|0[xX]([0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?\d+)[fFdD]?)",
      |        text,
      |        re.ASCII,
      |    ):
      |        raise ValueError("For input string: " + repr(text))
      |    if text[-1] in "dDfF":
      |        text = text[:-1]
      |    if "x" not in text.lower():
      |        return float(text)
      |    try:
      |        return float.fromhex(text)
      |    except OverflowError:
      |        return float("-inf") if text.startswith("-") else float("inf")
      |
      |def _texera_double_key(x):
      |    import math
      |
      |    if math.isnan(x):
      |        return (1, 0.0, 0.0)
      |    return (0, x, math.copysign(1.0, x))
      |
      |def _texera_key_codes(*frames, signed=False):
      |    import math
      |
      |    codes = {}
      |
      |    def key(v, missing):
      |        if missing:
      |            return None
      |        if isinstance(v, float):
      |            if math.isnan(v):
      |                return object()
      |            if signed:
      |                return (v, math.copysign(1.0, v))
      |        return v
      |
      |    def code(df):
      |        out = []
      |        for values, missing in zip(df.itertuples(index=False, name=None), df.isna().to_numpy()):
      |            row = tuple(key(v, m) for v, m in zip(values, missing))
      |            out.append(codes.setdefault(row, len(codes)))
      |        return pd.Series(out, index=df.index, dtype="int64")
      |
      |    return [code(df) for df in frames]
      |
      |def _texera_read_doubles(s):
      |    import numpy as np
      |
      |    if pd.api.types.is_numeric_dtype(s):
      |        return s
      |    holes = s.isna().to_numpy()
      |    values = np.array(
      |        [np.nan if hole else _texera_java_double(x) for x, hole in zip(s, holes)],
      |        dtype="float64",
      |    )
      |    if np.isnan(values[~holes]).any():
      |        values = pd.arrays.FloatingArray(values, holes)
      |    return pd.Series(values, index=s.index, name=s.name)
      |
      |def _texera_cast_string(s):
      |    # `toString` on the field, so the COLUMN's type decides the text and
      |    # not the shape of the value: a double keeps its point, whether or not
      |    # it lands on a whole number, and an integer never grows one. A column
      |    # of no single type is read value by value.
      |    if pd.api.types.is_bool_dtype(s):
      |        return s.map(lambda x: None if pd.isna(x) else ("true" if x else "false"))
      |    if pd.api.types.is_integer_dtype(s):
      |        return s.map(lambda x: None if pd.isna(x) else str(int(x)))
      |    if pd.api.types.is_datetime64_any_dtype(s):
      |        # java.sql.Timestamp.toString: trailing zeros dropped from the
      |        # fraction, but never all of them. Python's own str() writes no
      |        # fraction at all on a whole second and six digits otherwise.
      |        def _ts(x):
      |            if pd.isna(x):
      |                return None
      |            text = x.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0")
      |            return text + "0" if text.endswith(".") else text
      |
      |        return s.map(_ts)
      |    return s.map(
      |        lambda x: None
      |        if pd.isna(x)
      |        else ("true" if x else "false") if pd.api.types.is_bool(x) else str(x)
      |    )""".stripMargin
}
