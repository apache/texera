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

import os
import traceback

from core.models import ExceptionInfo
from core.util.console_message.timestamp import current_time_in_local_timezone
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ConsoleMessageType,
)


def create_error_console_message(
    worker_id: str, exc_info: ExceptionInfo
) -> ConsoleMessage:
    """Build an ERROR ``ConsoleMessage`` describing ``exc_info``.

    Produces the operator-facing error message for an uncaught exception,
    whether it surfaced from a UDF on the data path (DataProcessor) or from a
    user expression evaluated on the main loop thread. Sharing this factory
    keeps every uncaught-exception path reporting identically; callers are
    responsible for recording the exception with the exception manager,
    queueing the returned message, and flushing/pausing as appropriate.
    """
    tb = traceback.extract_tb(exc_info[2])
    if tb:
        filename, line_number, func_name, _ = tb[-1]
        module_name, _ = os.path.splitext(os.path.basename(filename))
        source = f"{module_name}:{func_name}:{line_number}"
    else:
        # No traceback frames (e.g. an exception object that was never raised).
        # Still report it -- an error reporter must not itself throw.
        source = ""
    formatted_exception = traceback.format_exception(*exc_info)
    title: str = formatted_exception[-1].strip()
    message: str = "\n".join(formatted_exception)

    return ConsoleMessage(
        worker_id=worker_id,
        timestamp=current_time_in_local_timezone(),
        msg_type=ConsoleMessageType.ERROR,
        source=source,
        title=title,
        message=message,
    )
