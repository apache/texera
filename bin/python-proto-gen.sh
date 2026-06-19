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

set -euo pipefail

# Resolve repo root from this script's location (avoids git/CWD assumptions
# so the script works inside Docker build stages before .git is copied).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEXERA_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
AMBER_DIR="$TEXERA_HOME/amber"
PYAMBER_DIR="$AMBER_DIR/src/main/python"
PROTOBUF_AMBER_DIR="$AMBER_DIR/src/main/protobuf"

CORE_DIR="$TEXERA_HOME/common/workflow-core"
PROTOBUF_CORE_DIR="$CORE_DIR/src/main/protobuf"

PROTOC_INCLUDE_DIR="$(dirname "$(dirname "$(command -v protoc)")")/include"

# proto-gen
mkdir -p "$PYAMBER_DIR/proto"
protoc --python_betterproto_out="$PYAMBER_DIR/proto" \
 -I="$PROTOC_INCLUDE_DIR" \
 -I="$PROTOBUF_AMBER_DIR" \
 -I="$PROTOBUF_CORE_DIR" \
 $(find "$PROTOBUF_AMBER_DIR" -iname "*.proto") \
 $(find "$PROTOBUF_CORE_DIR" -iname "*.proto")
