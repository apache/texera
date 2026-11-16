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

"""Run the generated `_read_binary_value` against column values at and over the size limit.

Reads the generated operator Python on stdin and writes, for each case,
{"raised": "<ExcType>", "message": ..., "calls": [...]} or
{"returned": <length>, "calls": [...]} on stdout as one JSON object.

The method and its size check are lifted out of the source and run on their
own (the generated module depends on the pytexera runtime). The helpers that
parse a value are replaced by recorders, so `calls` shows whether anything
looked at a value before it was refused.
"""
import base64
import json
import re
import sys

WANTED = ("_read_binary_value", "_check_column_value_size")


def lift(source, name):
    """Return the text of a 4-space-indented method, or '' when absent."""
    out, started = [], False
    for line in source.split("\n"):
        stripped = line.strip()
        if stripped.startswith("def " + name + "("):
            started = True
        elif started and (line.startswith("    def ") or (line and not line.startswith(" "))):
            break
        if started:
            out.append(line)
    return "\n".join(out)


def main():
    source = sys.stdin.read()
    expr = re.search(r"MAX_REMOTE_FETCH_BYTES = ([0-9 *]+)", source).group(1)
    limit = eval(expr, {"__builtins__": {}})
    calls = []

    class RecordingBase64:
        def b64decode(self, *args, **kwargs):
            calls.append("b64decode")
            return base64.b64decode(*args, **kwargs)

    class PandasStub:
        # Only pd.isna is used; enough of it for the values below.
        @staticmethod
        def isna(value):
            return value is None or (isinstance(value, float) and value != value)

    def recorder(name, result):
        def method(self, *args, **kwargs):
            calls.append(name)
            return result
        return method

    namespace = {"base64": RecordingBase64(), "pd": PandasStub()}
    methods = "\n".join(lift(source, name) for name in WANTED)
    exec("class Reader:\n    MAX_REMOTE_FETCH_BYTES = %d\n%s\n" % (limit, methods), namespace)
    reader_class = namespace["Reader"]
    reader_class._looks_like_html = recorder("_looks_like_html", False)
    reader_class._html_to_image_bytes = recorder("_html_to_image_bytes", b"")
    reader_class._fetch_remote_url = recorder("_fetch_remote_url", ("", b""))
    reader = reader_class()

    # "A" * n is valid base64 when n is a multiple of 4, so the at-limit string
    # goes through the ordinary decode path rather than the UTF-8 fallback.
    cases = {
        "str_at_limit": lambda: "A" * limit,
        "str_over_limit": lambda: "A" * (limit + 1),
        "bytes_at_limit": lambda: b"\0" * limit,
        "bytes_over_limit": lambda: b"\0" * (limit + 1),
    }
    results = {"limit": limit}
    for name, make in cases.items():
        del calls[:]
        try:
            value = reader._read_binary_value(make())
            results[name] = {"returned": len(value), "calls": list(calls)}
        except Exception as e:
            results[name] = {"raised": type(e).__name__, "message": str(e), "calls": list(calls)}
    json.dump(results, sys.stdout)


if __name__ == "__main__":
    main()
