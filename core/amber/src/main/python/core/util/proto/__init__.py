import re
from typing import T

from betterproto import Message, which_one_of

camel_case_pattern = re.compile(r"(?<!^)(?=[A-Z])")


def get_one_of(base: T, sealed=True) -> T:
    _, value = which_one_of(base, ("sealed_" if sealed else "") + "value")
    return value


def set_one_of(base: T, value: Message) -> T:
    name = value.__class__.__name__
    name = name.strip("V2")
    snake_case_name = re.sub(camel_case_pattern, "_", name).lower()
    ret = base()
    ret.__setattr__(snake_case_name, value)
    return ret


# implicitly used when being imported, this is to make betterproto
# Messages hashable.
Message.__hash__ = lambda x: hash(x.__repr__())
