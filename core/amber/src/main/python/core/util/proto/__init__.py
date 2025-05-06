# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
