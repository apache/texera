import re

worker_name_pattern = re.compile(r"Worker:WF\d+-.+-(\w+)-(\d+)")


def get_worker_index(worker_id: str) -> int:
    match = worker_name_pattern.match(worker_id)
    if match:
        return int(match.group(2))
    raise ValueError("Invalid worker ID format")
