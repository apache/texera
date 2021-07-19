import json


def serialize_arguments(*args, **kwargs):
    """
    serialize the given positional and keyword arguments into JSON.
    we are using JSON to support cross-language RPC. The result will
    be like `{"args": ["a", 1], "kwargs": {"x": 1, "y": "hello"}}`.
    :param args: positional arguments, order matters.
    :param kwargs: keyword arguments.
    :return:
    """
    return json.dumps({"args": args, "kwargs": kwargs}).encode("utf-8")


def deserialize_arguments(argument_bytes: bytes) -> dict:
    """
    deserialize the bytes into positional and keyword arguments.
    :param argument_bytes:
        JSON formatted bytes, e.g.,
        `{"args": ["a", 1], "kwargs": {"x": 1, "y": "hello"}}`;
        could be empty.
    :return: a dictionary of "args" and "kwargs" fields.
    """
    return json.loads(argument_bytes) if argument_bytes else {"args": [], "kwargs": {}}
