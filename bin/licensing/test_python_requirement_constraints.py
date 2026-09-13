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

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INSTALL_FILES = (
    ROOT / ".github/workflows/build.yml",
    ROOT / ".github/workflows/benchmarks.yml",
    ROOT / "bin/dockerfiles/computing-unit-master.dockerfile",
    ROOT / "bin/dockerfiles/computing-unit-worker.dockerfile",
)


def follow_up_installs(text: str) -> list[str]:
    return [
        line.strip()
        for line in text.splitlines()
        if re.search(r"-r (?:amber/|/tmp/)?(?:operator-|dev-)requirements\.txt", line)
    ]


class PythonRequirementConstraintTest(unittest.TestCase):
    def test_every_follow_up_install_constrains_runtime_requirements(self) -> None:
        installs = [
            (path.relative_to(ROOT), line)
            for path in INSTALL_FILES
            for line in follow_up_installs(path.read_text(encoding="utf-8"))
        ]
        self.assertTrue(installs, "the audit must discover follow-up requirement installs")
        unconstrained = [
            f"{path}: {line}"
            for path, line in installs
            if not re.search(r"-c (?:amber/|/tmp/)?requirements\.txt", line)
        ]
        self.assertFalse(
            unconstrained, "unconstrained follow-up installs:\n" + "\n".join(unconstrained)
        )

    def test_declared_typing_extensions_pin_matches_binary_manifest(self) -> None:
        requirements = (ROOT / "amber/requirements.txt").read_text(encoding="utf-8")
        license_binary = (ROOT / "amber/LICENSE-binary-python").read_text(encoding="utf-8")
        declared = re.search(r"^typing_extensions==([^\s]+)$", requirements, re.MULTILINE)
        recorded = re.search(
            r"^\s*- typing-extensions==([^\s]+)$", license_binary, re.MULTILINE
        )
        self.assertIsNotNone(declared)
        self.assertIsNotNone(recorded)
        self.assertEqual(declared.group(1), recorded.group(1))
