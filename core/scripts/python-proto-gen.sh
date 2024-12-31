# assuming inside the pytexera executing Python ENV

# dirs
TEXERA_ROOT="$(git rev-parse --show-toplevel)"
AMBER_DIR="$TEXERA_ROOT/core/amber"
PYAMBER_DIR="$AMBER_DIR/src/main/python"
PROTOBUF_AMBER_DIR="$AMBER_DIR/src/main/protobuf"

CORE_DIR="$TEXERA_ROOT/core/workflow-core"
PROTOBUF_CORE_DIR="$CORE_DIR/src/main/protobuf"

# proto-gen
protoc --python_betterproto_out="$PYAMBER_DIR/proto" \
 -I="$PROTOBUF_AMBER_DIR" \
 -I="$PROTOBUF_CORE_DIR" \
 $(find "$PROTOBUF_AMBER_DIR" -iname "*.proto") \
 $(find "$PROTOBUF_CORE_DIR" -iname "*.proto")
