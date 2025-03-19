import base64
import re

from proto.edu.uci.ics.amber.core import GlobalPortIdentity

worker_name_pattern = re.compile(r"Worker:WF\d+-.+-(\w+)-(\d+)")


def get_worker_index(worker_id: str) -> int:
    match = worker_name_pattern.match(worker_id)
    if match:
        return int(match.group(2))
    raise ValueError("Invalid worker ID format")


def serialize_global_port_identity(obj: GlobalPortIdentity) -> str:
    binary_data = obj.SerializeToString()
    # Base32 encoding produces an ASCII string without underscores
    return base64.b32encode(binary_data).decode("ascii")


def deserialize_global_port_identity(encoded_str: str) -> GlobalPortIdentity:
    binary_data = base64.b32decode(encoded_str)
    return GlobalPortIdentity.FromString(binary_data)
