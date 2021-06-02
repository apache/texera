import json


def serialize_arguments(*args, **kwargs):
    return json.dumps({"args": args, "kwargs": kwargs}).encode("utf-8")


def deserialize_arguments(argument_bytes: bytes):
    return json.loads(argument_bytes) if argument_bytes else {"args": [], "kwargs": {}}
