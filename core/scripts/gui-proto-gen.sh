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