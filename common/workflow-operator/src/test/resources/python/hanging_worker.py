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
  (default)            announce readiness, then never answer a request
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

    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    for _ in sys.stdin:
        _sleep_forever()


if __name__ == "__main__":
    main()
