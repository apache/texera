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
"""Run the generated Hugging Face response parser against real response shapes.

Reads {"source": <generated operator Python>, "task": <task tag>,
"bodies": [<parsed response>, ...]} on stdin and writes
{"results": [{"value": <cell text>} | {"raised": "<ExcType>"}, ...]} on stdout.

The generated module cannot be imported as-is (it depends on the pytexera
runtime), so the parser and its helpers are lifted out of the source and
executed on their own. That keeps the assertion about the emitted code itself
rather than a transcription of it.
"""
import io
import json
import re
import sys

# Exactly what _parse_response can reach: itself, the chat-content helper, and
# the data-URL helper used by the image-to-image branch. Anything else in the
# generated class belongs to the request loop, not to parsing.
WANTED = ("_parse_response", "_chat_message_content", "_url_to_data_url")


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
    request = json.load(sys.stdin)
    methods = [m for m in (lift(request["source"], n) for n in WANTED) if m.strip()]
    module = "import json\nclass Parser:\n    TASK = %r\n%s\n" % (
        request["task"],
        "\n".join(methods),
    )
    namespace = {}
    exec(compile(module, "<generated>", "exec"), namespace)  # noqa: S102 - the point of the probe
    parser = namespace["Parser"]()

    results = []
    for body in request["bodies"]:
        try:
            results.append({"value": parser._parse_response(body)})
        except Exception as exc:  # noqa: BLE001 - reporting the type is the assertion
            results.append({"raised": type(exc).__name__})
    json.dump({"results": results}, sys.stdout)


if __name__ == "__main__":
    main()
