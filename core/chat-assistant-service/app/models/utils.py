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

import json
from typing import List, Dict, Any, Optional


def parse_links(links: str) -> List[Dict[str, Any]]:
    """
    Parse `links`, which may be an empty string or a JSON array string.
    Returns a list of link dicts or an empty list.
    """
    if not links or not links.strip():
        return []
    try:
        data = json.loads(links)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def parse_operator_and_position(
    operator_and_position: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Parse `operator_and_position`, which may be None or a JSON object string.
    Returns a dict or None on failure.
    """
    if not operator_and_position:
        return None
    try:
        return json.loads(operator_and_position)
    except json.JSONDecodeError:
        return None
