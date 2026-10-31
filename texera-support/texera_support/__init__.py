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

import pandas as pd


def _texera_cast_boolean(x):
    if isinstance(x, str):
        text = _texera_java_trim(x)
        lowered = text.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        return _texera_parse_int(text, 32) == 1
    return x != 0

def _texera_parse_number(text):
    import re

    match = re.match(r"\s*(-?)([0-9,]*)(?:\.([0-9]*))?", text)
    sign = -1 if match.group(1) == "-" else 1
    digits = (match.group(2) or "").replace(",", "")
    fraction = match.group(3) or ""
    if not digits and not fraction:
        raise ValueError("Unparseable number: " + repr(text))
    if fraction:
        return sign * float((digits or "0") + "." + fraction), True
    value = sign * int(digits)
    if -(2 ** 63) <= value <= 2 ** 63 - 1:
        return value, False
    return float(value), True

def _texera_saturate(value, low, high):
    if value != value:
        return 0
    if value <= low:
        return low
    if value >= high:
        return high
    return int(value)

def _texera_cast_integral(x):
    if isinstance(x, str):
        x, is_double = _texera_parse_number(x)
        if not is_double:
            return x
    if isinstance(x, bool):
        return 1 if x else 0
    if isinstance(x, float):
        return _texera_saturate(x, -(2 ** 63), 2 ** 63 - 1)
    return int(x)

def _texera_cast_int32(x, wrap):
    if isinstance(x, str):
        value, is_double = _texera_parse_number(x)
        wrap = not is_double
    elif isinstance(x, bool):
        value = 1 if x else 0
    else:
        value = x
    if wrap:
        return ((int(value) + 2147483648) % 4294967296) - 2147483648
    return _texera_saturate(value, -2147483648, 2147483647)

def _texera_text_to_timestamp(s):
    from dateutil.parser import parse as _parse_date

    zone = _texera_local_zone()

    def _one(x):
        try:
            parsed = _parse_date(str(x).strip())
        except (ValueError, OverflowError) as e:
            raise ValueError(f"Failed to parse type str to Timestamp: {x}") from e
        if parsed.tzinfo is not None:
            parsed = _texera_local_wall_clock(parsed, zone)
        return parsed.replace(microsecond=parsed.microsecond // 1000 * 1000)

    text = s.astype(str).str.strip()
    plain = s.notna() & text.str.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?)?"
    )
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[us]")
    out[plain] = pd.to_datetime(text[plain], format="ISO8601", errors="coerce").dt.floor("ms")
    rest = s.notna() & out.isna()
    out[rest] = s[rest].map(_one).astype("datetime64[us]")
    return out

def _texera_cast_timestamps(s):
    if not pd.api.types.is_datetime64_any_dtype(s):
        return _texera_text_to_timestamp(s)
    if getattr(s.dtype, "tz", None) is not None:
        return _texera_local_wall_clocks(s)
    return s

def _texera_epoch_millis_to_timestamp(s):
    return _texera_local_wall_clocks(pd.to_datetime(s, unit="ms", errors="coerce", utc=True))

def _texera_local_zone():
    import os
    import zoneinfo

    names = [os.environ.get("TZ", "").lstrip(":")]
    try:
        import tzlocal

        names.insert(0, str(tzlocal.get_localzone()))
    except Exception:
        pass
    link = os.path.realpath("/etc/localtime")
    if "zoneinfo/" in link:
        names.append(link.split("zoneinfo/", 1)[1])
    for name in names:
        try:
            return zoneinfo.ZoneInfo(name)
        except (ValueError, KeyError):
            continue
    return None

def _texera_standard_offset(zone):
    import datetime
    import time

    if zone is None:
        return datetime.timedelta(seconds=-time.timezone)
    now = datetime.datetime.now(zone)
    return now.utcoffset() - now.dst()

def _texera_local_wall_clock(moment, zone):
    import datetime

    from dateutil.tz import tzlocal

    utc = moment.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    if utc < datetime.datetime(1900, 1, 1):
        return utc + _texera_standard_offset(zone)
    if zone is not None:
        return moment.astimezone(zone).replace(tzinfo=None)
    try:
        return moment.astimezone(tzlocal()).replace(tzinfo=None)
    except (OSError, OverflowError):
        return utc + _texera_standard_offset(zone)

def _texera_local_wall_clocks(s):
    import datetime

    from dateutil.tz import tzlocal

    zone = _texera_local_zone()
    utc = s.dt.tz_convert("UTC").dt.tz_localize(None)
    if zone is not None:
        wall = s.dt.tz_convert(zone).dt.tz_localize(None)
        before = utc < pd.Timestamp(1900, 1, 1)
        return wall.where(~before, utc + _texera_standard_offset(zone))
    try:
        return s.dt.tz_convert(tzlocal()).dt.tz_localize(None)
    except (OSError, OverflowError):
        pass

    def _offset(t):
        if pd.isna(t):
            return 0
        moment = t.to_pydatetime(warn=False)
        utc = moment.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return int((_texera_local_wall_clock(moment, None) - utc).total_seconds())

    seconds = pd.Series([_offset(t) for t in s], index=s.index)
    return utc + pd.to_timedelta(seconds, unit="s").astype("timedelta64[s]")

def _texera_wall_clock_instants(s):
    from dateutil.tz import tzlocal

    walls = s.dt.floor("ms").astype("datetime64[ms]")
    zone = _texera_local_zone()
    standard = walls - _texera_standard_offset(zone)
    before = standard < pd.Timestamp(1900, 1, 1)

    def localized(t):
        return (
            t.dt.tz_localize(zone or tzlocal(), ambiguous=False, nonexistent=pd.Timedelta("1h"))
            .dt.tz_convert("UTC")
            .dt.tz_localize(None)
        )

    try:
        return localized(walls).where(~before, standard)
    except (OSError, OverflowError):
        pass

    def one(i):
        if before.iloc[i]:
            return standard.iloc[i]
        try:
            return localized(walls.iloc[i : i + 1]).iloc[0]
        except (OSError, OverflowError):
            return standard.iloc[i]

    return pd.Series([one(i) for i in range(len(walls))], index=s.index, dtype="datetime64[ms]")

def _texera_cast_double(x):
    return _texera_java_double(x) if isinstance(x, str) else float(x)

def _texera_cast_doubles(s):
    holes = s.isna().to_numpy()
    values = _texera_cast_cells(s, _texera_cast_double, 0.0).to_numpy(dtype="float64")
    return pd.Series(pd.arrays.FloatingArray(values, holes), index=s.index, name=s.name)

def _texera_java_text(x):
    import datetime
    import decimal

    if pd.api.types.is_bool(x):
        return "true" if x else "false"
    if isinstance(x, datetime.datetime):
        nanos = x.microsecond * 1000 + getattr(x, "nanosecond", 0)
        return x.strftime("%Y-%m-%d %H:%M:%S") + "." + (f"{nanos:09d}".rstrip("0") or "0")
    if pd.api.types.is_integer(x):
        return str(int(x))
    if pd.api.types.is_float(x):
        value = float(x)
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0 or 1e-3 <= abs(value) < 1e7:
            return repr(value)
        sign, digits, exponent = decimal.Decimal(repr(value)).as_tuple()
        power = len(digits) + exponent - 1
        kept = "".join(map(str, digits)).rstrip("0") or "0"
        return ("-" if sign else "") + kept[0] + "." + (kept[1:] or "0") + "E" + str(power)
    return str(x)

def _texera_java_trim(text):
    return text.strip("".join(map(chr, range(33))))

def _texera_parse_int(text, bits):
    import re

    text = _texera_java_trim(text)
    bound = 2 ** (bits - 1)
    if not re.fullmatch(r"[+-]?\d+", text) or not -bound <= int(text) < bound:
        raise ValueError("For input string: " + repr(text))
    return int(text)

def _texera_java_double(text):
    import re

    text = _texera_java_trim(text)
    if not re.fullmatch(
        r"[+-]?(NaN|Infinity|((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?"
        r"|0[xX]([0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?\d+)[fFdD]?)",
        text,
        re.ASCII,
    ):
        raise ValueError("For input string: " + repr(text))
    if text[-1] in "dDfF":
        text = text[:-1]
    if "x" not in text.lower():
        return float(text)
    try:
        return float.fromhex(text)
    except OverflowError:
        return float("-inf") if text.startswith("-") else float("inf")

def _texera_double_key(x):
    import math

    if math.isnan(x):
        return (1, 0.0, 0.0)
    return (0, x, math.copysign(1.0, x))

def _texera_key_codes(*frames, signed=False):
    import math

    codes = {}

    def key(v, missing):
        if missing:
            return None
        if isinstance(v, float):
            if math.isnan(v):
                return object()
            if signed:
                return (v, math.copysign(1.0, v))
        return v

    def code(df):
        out = []
        for values, missing in zip(df.itertuples(index=False, name=None), df.isna().to_numpy()):
            row = tuple(key(v, m) for v, m in zip(values, missing))
            out.append(codes.setdefault(row, len(codes)))
        return pd.Series(out, index=df.index, dtype="int64")

    return [code(df) for df in frames]

def _texera_read_doubles(s):
    import numpy as np

    if pd.api.types.is_numeric_dtype(s):
        return s
    holes = s.isna().to_numpy()
    values = np.array(
        [np.nan if hole else _texera_java_double(x) for x, hole in zip(s, holes)],
        dtype="float64",
    )
    if np.isnan(values[~holes]).any():
        values = pd.arrays.FloatingArray(values, holes)
    return pd.Series(values, index=s.index, name=s.name)

def _texera_cast_string(s):
    return _texera_cast_cells(s, _texera_java_text, None)

def _texera_cast_cells(s, cast, hole):
    holes = s.isna().to_numpy()
    return pd.Series(
        [hole if empty else cast(x) for x, empty in zip(s, holes)],
        index=s.index,
        name=s.name,
        dtype=object,
    )


class _TexeraJavaRandom:
    _MASK = (1 << 48) - 1
    _MULTIPLIER = 0x5DEECE66D
    _ADDEND = 0xB

    def __init__(self, seed):
        self._seed = (seed ^ self._MULTIPLIER) & self._MASK

    def _next(self, bits):
        self._seed = (self._seed * self._MULTIPLIER + self._ADDEND) & self._MASK
        value = self._seed >> (48 - bits)
        return value - (1 << 32) if value >= (1 << 31) else value

    def next_double(self):
        return ((self._next(26) << 27) + self._next(27)) * (2.0 ** -53)

    def next_int(self, bound):
        if bound <= 0:
            raise ValueError("bound must be positive")
        if bound & (-bound) == bound:
            return (bound * self._next(31)) >> 31
        while True:
            bits = self._next(31)
            value = bits % bound
            probe = bits - value + (bound - 1)
            if ((probe + (1 << 31)) % (1 << 32)) - (1 << 31) >= 0:
                return value


def _texera_agg_concat(series):
    partial = ""
    for v, missing in zip(series, series.isna()):
        text = "" if missing else _texera_java_text(v)
        partial = text if partial == "" else partial + "," + text
    return partial

def _texera_agg_int_sum(series):
    total = int(series.sum())
    return ((total + (1 << 31)) % (1 << 32)) - (1 << 31)

def _texera_agg_ts_epoch_ms(series):
    return _texera_wall_clock_instants(series.dropna()).astype("int64")

def _texera_agg_ts_sum(series):
    total = int(_texera_agg_ts_epoch_ms(series).astype(object).sum())
    total = ((total + (1 << 63)) % (1 << 64)) - (1 << 63)
    import datetime

    utc = pd.Timestamp(total, unit="ms")
    zone = _texera_local_zone()
    try:
        moment = utc.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
        shift = _texera_local_wall_clock(moment, zone) - moment.replace(tzinfo=None)
    except (OSError, OverflowError):
        shift = _texera_standard_offset(zone)
    return utc + shift

def _texera_agg_ts_mean(series):
    kept = _texera_agg_ts_epoch_ms(series)
    if len(kept) == 0:
        return None
    return float(kept.astype(object).sum()) / len(kept)

def _texera_agg_ts_pick(series, better):
    kept = series.dropna()
    if len(kept) == 0:
        return pd.NaT
    millis = kept.dt.floor("ms")
    pick = 0
    for i in range(1, len(kept)):
        if better(millis.iloc[i], millis.iloc[pick]):
            pick = i
    return kept.iloc[pick]

def _texera_agg_ts_min(series):
    return _texera_agg_ts_pick(series, lambda a, b: a < b)

def _texera_agg_ts_max(series):
    return _texera_agg_ts_pick(series, lambda a, b: a > b)

def _texera_agg_double_values(series):
    return [float(v) for v in series.dropna()]

def _texera_agg_double_sum(series):
    partial = 0.0
    for v in _texera_agg_double_values(series):
        partial += v
    return partial

def _texera_agg_double_mean(series):
    kept = _texera_agg_double_values(series)
    if not kept:
        return pd.NA
    return _texera_agg_double_sum(series) / len(kept)

def _texera_agg_double_min(series):
    start = 1.7976931348623157e308
    partial = start
    for v in _texera_agg_double_values(series):
        if _texera_double_key(v) < _texera_double_key(partial):
            partial = v
    return pd.NA if partial == start else partial

def _texera_agg_double_max(series):
    start = float("-inf")
    partial = start
    for v in _texera_agg_double_values(series):
        if _texera_double_key(v) > _texera_double_key(partial):
            partial = v
    return pd.NA if partial == start else partial

def _texera_agg_double_by_group(column, fold):
    results = column.apply(fold)
    mask = results.map(lambda v: v is pd.NA).to_numpy(bool)
    values = results.where(~mask, 0.0).to_numpy(float)
    return pd.Series(pd.arrays.FloatingArray(values, mask), index=results.index)


def _texera_filter_text_key(s):
    return s.encode("utf-16-be", "surrogatepass")

def _texera_filter_compared(left, right):
    return (left > right) - (left < right)

def _texera_filter_rows(column, condition, order):
    passes = {
        "=": lambda c: c == 0,
        ">": lambda c: c > 0,
        ">=": lambda c: c >= 0,
        "<": lambda c: c < 0,
        "<=": lambda c: c <= 0,
        "!=": lambda c: c != 0,
    }[condition]
    return pd.Series(
        [
            False if missing else passes(order(v))
            for v, missing in zip(column, column.isna())
        ],
        index=column.index,
        dtype=bool,
    )

def _texera_filter_keep_double(column, condition, value):
    target = _texera_double_key(_texera_java_double(value))

    def order(v):
        return _texera_filter_compared(_texera_double_key(float(v)), target)

    return _texera_filter_rows(column, condition, order)

def _texera_filter_keep_long(column, condition, value):
    target = _texera_parse_int(value, 64)

    def order(v):
        return _texera_filter_compared(int(v), target)

    return _texera_filter_rows(column, condition, order)

def _texera_filter_keep_boolean(column, condition, value):
    target = _texera_filter_text_key(_texera_java_trim(value).lower())

    def order(v):
        return _texera_filter_compared(_texera_filter_text_key("true" if v else "false"), target)

    return _texera_filter_rows(column, condition, order)

def _texera_filter_keep_timestamp(column, condition, value):
    zone = _texera_local_zone()

    def wall_clock(ts):
        ts = pd.Timestamp(ts).floor("ms")
        if ts.tzinfo is None:
            return ts
        return pd.Timestamp(_texera_local_wall_clock(ts.to_pydatetime(), zone))

    target = wall_clock(_texera_java_trim(value))

    def order(v):
        return _texera_filter_compared(wall_clock(v), target)

    return _texera_filter_rows(column, condition, order)

def _texera_filter_keep_string(column, condition, value):
    try:
        value_number = _texera_double_key(_texera_java_double(value))
    except ValueError:
        value_number = None

    def order(v):
        text = str(v)
        if value_number is not None:
            try:
                number = _texera_double_key(_texera_java_double(text))
                return _texera_filter_compared(number, value_number)
            except ValueError:
                pass
        return _texera_filter_compared(
            _texera_filter_text_key(text), _texera_filter_text_key(value)
        )

    return _texera_filter_rows(column, condition, order)

def _texera_filter_keep(column, condition, value):
    dtype = column.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return _texera_filter_keep_boolean(column, condition, value)
    if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
        return _texera_filter_keep_double(column, condition, value)
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return _texera_filter_keep_timestamp(column, condition, value)
    return _texera_filter_keep_string(column, condition, value)


def _texera_decode(raw, encoding):
    if encoding == "utf-16":
        if raw[:2] == b"\xfe\xff":
            return raw[2:].decode("utf-16-be", "replace")
        if raw[:2] == b"\xff\xfe":
            return raw[2:].decode("utf-16-le", "replace")
        return raw.decode("utf-16-be", "replace")
    return raw.decode(encoding, "replace")


def _texera_json_flatten(record):
    flat = {}
    stack = [(record, "")]
    while stack:
        node, parent = stack.pop()
        if isinstance(node, dict):
            for key, child in node.items():
                path = parent + "." + key if parent else key
                if isinstance(child, (dict, list)):
                    stack.append((child, path))
                else:
                    flat[path] = child
        elif isinstance(node, list):
            for index, child in enumerate(node):
                stack.append((child, parent + str(index + 1)))
        elif parent:
            flat[parent] = node
    return flat


def _texera_json_text(value):
    if value is None:
        return "null"
    if isinstance(value, (bool, int, float)):
        return _texera_java_text(value)
    if isinstance(value, str):
        return value
    return None


def _texera_python_operator_input(df):
    df = df.copy()
    for _c in df.columns:
        if isinstance(df[_c].dtype, (pd.Float64Dtype, pd.Float32Dtype)):
            df[_c] = df[_c].astype("float64")
    return df
