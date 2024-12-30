# assuming inside the pytexera executing Python ENV

# dirs
TEXERA_ROOT="$(git rev-parse --show-toplevel)"
AMBER_DIR="$TEXERA_ROOT/core/amber"
PYAMBER_DIR="$AMBER_DIR/src/main/python"
PROTOBUF_DIR_AMBER="$AMBER_DIR/src/main/protobuf"

CORE_DIR="$TEXERA_ROOT/core/workflow-core"
PROTOBUF_DIR_CORE="$CORE_DIR/src/main/protobuf"

# proto-gen
protoc --python_betterproto_out="$PYAMBER_DIR/proto" \
 -I="$PROTOBUF_DIR_AMBER" \
 -I="$PROTOBUF_DIR_CORE" \
 $(find "$PROTOBUF_DIR_AMBER" -iname "*.proto") \
 $(find "$PROTOBUF_DIR_CORE" -iname "*.proto")
