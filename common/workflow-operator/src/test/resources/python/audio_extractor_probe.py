#
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
#
"""Run the generated Audio Attribute Extractor against synthesised audio.

Reads {"source": <generated operator Python>, "cases": [{"name", "input"}]} on stdin
and writes {"miniaudio": bool, "results": [{"name", "rows" | "raised", "prints",
"largest_buffer"}], "close_prints": [...]} on stdout. `largest_buffer` is the biggest
numpy.empty request in bytes, so a test can tell a header-sized allocation apart. A case
whose input has "trace_memory": "true" also gets `peak_memory`, the traced peak in bytes.

The generated module is executed whole. `from pytexera import *` resolves to the
small stand-ins below (operator base class, tuple, large-binary reference and
stream), so the test needs numpy, scipy and, for compressed formats, miniaudio,
but not the Texera engine or its generated protobuf bindings.
"""

import base64
import contextlib
import io
import json
import sys
import tracemalloc
import types
import wave
from typing import Iterator, Optional

import numpy as np


class _Tuple:
    def __init__(self, fields):
        self._fields = dict(fields)

    def __getitem__(self, key):
        return self._fields[key]

    def as_dict(self):
        return dict(self._fields)

    def get_field_names(self):
        return tuple(self._fields)


class _LargeBinary:
    def __init__(self, data, storage_error=None):
        self.data = data
        self.storage_error = storage_error


class _Sized:
    """Stands in for a huge read without allocating it: only its length is real."""

    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n


class _StorageFailure:
    """A stream whose first read fails the way S3 does, with the given error code."""

    def __init__(self, code):
        self.code = code

    def read(self, n=-1):
        from botocore.exceptions import ClientError

        raise ClientError(
            {"Error": {"Code": self.code, "Message": "probe"}}, "GetObject"
        )


class _Endless:
    def read(self, n=-1):
        return _Sized(n)


class _LargeBinaryInputStream:
    def __init__(self, ref):
        self.ref = ref

    def __enter__(self):
        if self.ref.storage_error:
            return _StorageFailure(self.ref.storage_error)
        return _Endless() if self.ref.data is None else io.BytesIO(self.ref.data)

    def __exit__(self, *exc):
        return False


class _UDFOperatorV2:
    def open(self):
        pass

    def close(self):
        pass

    def decode_python_template(self, data):
        # Same as pytexera's Operator: user text reaches the code base64-encoded.
        raw = data.encode("ascii") if isinstance(data, str) else data
        return base64.b64decode(raw, validate=False).decode("utf-8", errors="strict")


stub = types.ModuleType("pytexera")
stub.__dict__.update(
    UDFOperatorV2=_UDFOperatorV2,
    Tuple=_Tuple,
    TupleLike=dict,
    Iterator=Iterator,
    Optional=Optional,
    overrides=lambda f: f,
    largebinary=_LargeBinary,
    LargeBinaryInputStream=_LargeBinaryInputStream,
)
stub.__all__ = list(
    stub.__dict__.keys()
    - {"__name__", "__doc__", "__package__", "__loader__", "__spec__"}
)
sys.modules["pytexera"] = stub


def wav_bytes(signal, sr, width=2, channels=1):
    x = np.clip(np.asarray(signal, dtype=np.float64), -1, 1)
    x = np.repeat(x, channels)
    if width == 1:
        data = ((x * 127) + 128).astype(np.uint8).tobytes()
    elif width == 2:
        data = (x * 32767).astype("<i2").tobytes()
    elif width == 3:
        v = (x * 8388607).astype("<i4").view(np.uint8).reshape(-1, 4)[:, :3]
        data = v.tobytes()
    else:
        data = (x * 2147483647).astype("<i4").tobytes()
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(channels)
        w.setsampwidth(width)
        w.setframerate(sr)
        w.writeframes(data)
    return buf.getvalue()


def synth(spec):
    kind = spec["kind"]
    if kind == "none":
        return None
    if kind == "text":
        return spec["text"]
    if kind == "raw":
        return base64.b64decode(spec["b64"])
    if kind == "large":
        inner = synth(spec["inner"]) if "inner" in spec else None
        return _LargeBinary(inner, spec.get("storage_error"))
    sr = spec.get("sr", 22050)
    n = int(spec.get("secs", 1.0) * sr)
    t = np.arange(n) / sr
    if kind == "tone":
        x = spec.get("amp", 0.5) * np.sin(2 * np.pi * spec["freq"] * t)
    elif kind == "silence":
        x = np.zeros(n)
    elif kind == "noise":
        x = spec.get("amp", 0.3) * np.random.default_rng(7).standard_normal(n)
    elif kind == "clicks":
        x = np.zeros(n)
        m = int(0.01 * sr)
        for c in spec["times"]:
            i = int(c * sr)
            x[i : i + m] += (
                0.8 * np.sin(2 * np.pi * 1000 * t[:m]) * np.linspace(1, 0, m)
            )
    else:
        raise ValueError(kind)
    data = bytearray(wav_bytes(x, sr, spec.get("width", 2), spec.get("channels", 1)))
    if spec.get(
        "sizes"
    ):  # RIFF and data chunk sizes, e.g. 0xFFFFFFFF as written to a pipe
        data[4:8] = spec["sizes"][0].to_bytes(4, "little")
        data[40:44] = spec["sizes"][1].to_bytes(4, "little")
    if "keep" in spec:
        data = data[: int(len(data) * spec["keep"])]
    if "drop" in spec:
        data = data[: len(data) - spec["drop"]]
    return bytes(data)


def inject(ns, kind):
    """Make the generated module fail part-way through a file; returns an undo."""
    if kind == "decoder_error":  # the native decoder gives up after its first chunk
        real = ns["_miniaudio_stream"]

        def failing(*args, **kwargs):
            stream = real(*args, **kwargs)
            yield next(stream)
            raise ns["miniaudio"].MiniaudioError("injected decoder failure")

        ns["_miniaudio_stream"] = failing
        return lambda: ns.__setitem__("_miniaudio_stream", real)
    if kind == "processing_error":  # a bug in our own code, after decoding has begun
        resampler = ns["_Resampler"]
        real, calls = resampler.feed, [0]

        def feed(self, chunk):
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError("injected processing bug")
            return real(self, chunk)

        resampler.feed = feed
        return lambda: setattr(resampler, "feed", real)
    if kind == "no_scipy":  # a computing unit without SciPy installed
        real = ns["resample_poly"]
        ns["resample_poly"] = None
        return lambda: ns.__setitem__("resample_poly", real)
    raise ValueError(kind)


def main():
    request = json.loads(sys.stdin.read())
    try:
        import miniaudio  # noqa: F401

        has_miniaudio = True
    except ImportError:
        has_miniaudio = False
    largest = [0]
    real_empty = np.empty

    def watched_empty(shape, *args, **kwargs):
        out = real_empty(shape, *args, **kwargs)
        largest[0] = max(largest[0], out.nbytes)
        return out

    np.empty = watched_empty  # the generated module shares this numpy module
    ns = {"__name__": "generated_audio_operator"}
    exec(compile(request["source"], "generated_audio_operator.py", "exec"), ns)
    op = ns["ProcessTupleOperator"]()
    op.open()
    results = []
    for case in request["cases"]:
        column = case["input"].get("column", "audio")
        fields = {"filename": case["name"], column: synth(case["input"])}
        out = io.StringIO()
        largest[0] = 0
        undo = (
            inject(ns, case["input"]["inject"]) if "inject" in case["input"] else None
        )
        traced = case["input"].get("trace_memory") == "true"
        if traced:  # numpy reports its buffers to tracemalloc
            tracemalloc.start()
        try:
            with contextlib.redirect_stdout(out):
                rows = list(op.process_tuple(_Tuple(fields), 0))
            result = {"name": case["name"], "rows": rows}
        except Exception as e:  # bad audio raises unless Skip Unreadable Files is on
            result = {"name": case["name"], "raised": f"{type(e).__name__}: {e}"}
        if traced:
            result["peak_memory"] = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        if undo:
            undo()
        result.update(prints=out.getvalue().splitlines(), largest_buffer=largest[0])
        results.append(result)
    closing = io.StringIO()
    with contextlib.redirect_stdout(closing):
        op.close()
    print(
        json.dumps(
            {
                "miniaudio": has_miniaudio,
                "results": results,
                "close_prints": closing.getvalue().splitlines(),
            }
        )
    )


main()
