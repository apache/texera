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
"""Run the generated provider lookup and request loop against a stubbed HF API.

Reads {"source": <generated operator Python>, "model": <model id>,
"cases": [{"task": ..., "hub": {"status": <int>, "body": <json>} | {"raise": true}}, ...]}
on stdin and writes {"results": [{"posts": [{"url": ..., "model": ...}, ...]}, ...]}
on stdout.

`_resolve_providers`, `_post_with_fallback` and `_post_pipeline` are lifted out
of the source and run on their own (the generated module depends on the
pytexera runtime). Only `requests` is replaced: GET answers with the case's Hub
response, POST records the URL and model it was sent and answers 200, so no
request leaves the machine.
"""

import json
import sys

WANTED = ("_resolve_providers", "_post_with_fallback", "_post_pipeline")
CONSTANTS = ("PROVIDER_COST_PRIORITY", "CHAT_ROUTES")


def lift(source, name):
    """Return the text of a 4-space-indented method, or '' when absent."""
    out, started = [], False
    for line in source.split("\n"):
        stripped = line.strip()
        if stripped.startswith("def " + name + "("):
            started = True
        elif started and (
            line.startswith("    def ") or (line and not line.startswith(" "))
        ):
            break
        if started:
            out.append(line)
    return "\n".join(out)


def lift_constant(source, name):
    """Return a multi-line class constant, up to its closing bracket."""
    out, started = [], False
    for line in source.split("\n"):
        if line.startswith("    " + name + " = "):
            started = True
        if started:
            out.append(line)
            if line in ("    ]", "    }"):
                break
    return "\n".join(out)


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class FakeRequests:
    def __init__(self, hub):
        self.hub = hub
        self.posts = []

    def get(self, url, **kwargs):
        if self.hub.get("raise"):
            raise ConnectionError("hub unreachable")
        return FakeResponse(self.hub["status"], self.hub.get("body", {}))

    def post(self, url, json=None, **kwargs):
        self.posts.append({"url": url, "model": (json or {}).get("model", "")})
        return FakeResponse(200, {})


def main():
    request = json.load(sys.stdin)
    source = request["source"]
    body = "\n".join(lift_constant(source, n) for n in CONSTANTS)
    body += "\n" + "\n".join(lift(source, n) for n in WANTED)
    module = "class Operator:\n%s\n" % body
    model = request["model"]

    results = []
    for case in request["cases"]:
        fake = FakeRequests(case["hub"])
        namespace = {"requests": fake}
        exec(compile(module, "<generated>", "exec"), namespace)  # noqa: S102 - the point of the probe
        op = namespace["Operator"]()
        op.TASK = case["task"]
        op.MODEL_ID = model
        if case["task"] in ("text-generation", "image-text-to-text"):
            payload = {"model": model, "messages": [{"role": "user", "content": "hi"}]}
        else:
            payload = {"inputs": "hi"}
        providers = op._resolve_providers("token")
        op._post_with_fallback(providers, {}, {}, payload, False, "hi")
        results.append({"posts": fake.posts})
    json.dump({"results": results}, sys.stdout)


if __name__ == "__main__":
    main()
