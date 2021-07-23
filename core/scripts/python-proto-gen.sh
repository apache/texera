source ../venv/bin/activate

protoc --python_betterproto_out=../amber/src/main/python/proto -I=../amber/src/main/protobuf  $(find ../amber/src/main/protobuf -iname "*.proto") --proto_path=../amber/src/main/protobuf
