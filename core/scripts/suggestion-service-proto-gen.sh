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

#!/bin/bash

TEXERA_ROOT="$(git rev-parse --show-toplevel)"
AMBER_DIR="$TEXERA_ROOT/core/amber"
PROTOBUF_AMBER_DIR="$AMBER_DIR/src/main/protobuf"
CORE_DIR="$TEXERA_ROOT/core/workflow-core"
PROTOBUF_CORE_DIR="$CORE_DIR/src/main/protobuf"

OUT_DIR="$TEXERA_ROOT/core/suggestion-service/model/proto"

# target proto
TABLEPROFILE_PROTO=$(find "$PROTOBUF_AMBER_DIR" -iname "tableprofile.proto")

# generate only that file
protoc --python_betterproto_out="$OUT_DIR" \
 -I="$PROTOBUF_AMBER_DIR" \
 -I="$PROTOBUF_CORE_DIR" \
 "$TABLEPROFILE_PROTO"