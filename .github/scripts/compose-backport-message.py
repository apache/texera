# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Composes the backport commit message: insert "(backported from commit X)"
# between the message body and the trailer block (the trailing run of
# `Key: value` lines such as Co-Authored-By and Signed-off-by) so trailers
# stay contiguous at the bottom — that's where git itself parses them.
#
# The trailer block, by git convention, is the run of `Key: value` lines
# after the LAST blank line in the message, and only counts if EVERY line
# after that blank line is in trailer format. This avoids mis-detecting a
# Conventional Commits subject like "feat: foo" or a body line like
# "References:" as a trailer.
#
# Usage: original-message-on-stdin | compose-backport-message.py <merge-sha>

import re
import sys

sha = sys.argv[1]
msg = sys.stdin.read().rstrip("\n")
lines = msg.split("\n")
trailer_re = re.compile(r"^[A-Za-z][A-Za-z0-9-]*:\s")

last_blank = -1
for idx in range(len(lines) - 1, -1, -1):
    if lines[idx] == "":
        last_blank = idx
        break

trailer_start = len(lines)
if last_blank != -1:
    candidate = lines[last_blank + 1 :]
    if candidate and all(trailer_re.match(line) for line in candidate):
        trailer_start = last_blank + 1

backport = f"(backported from commit {sha})"
if trailer_start == len(lines):
    print(msg + "\n\n" + backport)
else:
    body = "\n".join(lines[:trailer_start]).rstrip("\n")
    trailers = "\n".join(lines[trailer_start:])
    print(body + "\n\n" + backport + "\n\n" + trailers)
