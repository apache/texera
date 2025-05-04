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

TEXERA_ROOT="$(git rev-parse --show-toplevel)"
GUI_DIR="$TEXERA_ROOT/core/gui"
PROTOBUF_DIR="$TEXERA_ROOT/core/amber/src/main/protobuf"
GUI_PROTO_DIR="$GUI_DIR/src/app/common/type"

WORKFLOW_PROTO=$(find "$PROTOBUF_DIR" -iname "workflow.proto")
VIRTUALIDENTITY_PROTO=$(find "$PROTOBUF_DIR" -iname "virtualidentity.proto")

protoc --plugin="$GUI_DIR/node_modules/.bin/protoc-gen-ts_proto" \
  --ts_proto_out="$GUI_PROTO_DIR/proto" \
  -I="$PROTOBUF_DIR" \
  "$WORKFLOW_PROTO" \
  "$VIRTUALIDENTITY_PROTO" \
  --proto_path="$PROTOBUF_DIR"