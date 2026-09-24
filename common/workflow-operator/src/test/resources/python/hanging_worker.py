#!/usr/bin/env python3
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
"""
Test fixture: a worker that stays alive and stops talking, the failure a pool
timeout exists for. A crash is not a substitute — that closes the pipe and the
read returns on its own.

  --hang-before-ready  never announce readiness
  --deaf               announce readiness, then never read stdin at all, which
                       blocks the parent's write once the request outgrows the
                       pipe buffer
  --babble             announce a line that is not the protocol at all
  (default)            announce readiness, then answer every request except one
                       asking to hang: {"hang": true} is read and never answered.
                       {"drop-exit": true} is answered without an exit code

Whether a request is answered is a property of the request, not of a flag,
because the pool keys a sub-pool by script, args and env: a test that needs a
hung worker and a healthy job in the *same* sub-pool cannot get them from two
different launches.
"""
from __future__ import annotations

import json
import sys
import time


def _sleep_forever() -> None:
    # Outlives any timeout a test sets, and the pool kills this process, so the
    # sleep is what makes the worker unresponsive rather than slow.
    while True:
        time.sleep(3600)


def main() -> None:
    if "--hang-before-ready" in sys.argv:
        _sleep_forever()

    if "--babble" in sys.argv:
        # Then stay alive: it is the parent that has to end this process, since
        # nothing else would reap a worker rejected before it joined the pool.
        sys.stdout.write("not a protocol line\n")
        sys.stdout.flush()
        _sleep_forever()

    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    if "--deaf" in sys.argv:
        _sleep_forever()

    for line in sys.stdin:
        try:
            request = json.loads(line)
        except ValueError:
            request = {}
        if request.get("hang"):
            _sleep_forever()
        if request.get("drop-exit"):
            # Parses as the protocol, but leaves out the one field the parent
            # cannot supply for itself.
            sys.stdout.write(json.dumps({"stdout": "", "stderr": ""}) + "\n")
            sys.stdout.flush()
            continue
        sys.stdout.write(json.dumps({"exit": 0, "stdout": "", "stderr": ""}) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
