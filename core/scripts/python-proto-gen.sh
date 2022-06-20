# assuming inside the pytexera executing Python ENV

# dirs
TEXERA_ROOT="$(git rev-parse --show-toplevel)"
AMBER_DIR="$TEXERA_ROOT/core/amber"
PYAMBER_DIR="$AMBER_DIR/src/main/python"
PROTOBUF_DIR="$AMBER_DIR/src/main/protobuf"

# proto-gen
protoc --python_betterproto_out="$PYAMBER_DIR/proto" -I="$PROTOBUF_DIR"  $(find "$PROTOBUF_DIR" -iname "*.proto") --proto_path="$PROTOBUF_DIR"
