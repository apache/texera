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

"""pytest fixtures for the local-dev TUI tests.

The TUI sits behind `bin/local-dev.sh -i` rather than being a Python
package, so we load it once here with `importlib.util` and expose the
resulting module via the `tui` fixture. `sys.modules` registration is
required so the module's `@dataclass` decorators can resolve `__module__`
lookups."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
TUI_PATH = REPO_ROOT / "bin" / "local-dev" / "tui.py"


@pytest.fixture(scope="session")
def tui():
    spec = importlib.util.spec_from_file_location("local_dev_tui", TUI_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["local_dev_tui"] = module
    spec.loader.exec_module(module)
    return module
