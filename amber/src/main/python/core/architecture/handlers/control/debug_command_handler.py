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

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    DebugCommandRequest,
)


class WorkerDebugCommandHandler(ControlHandler):
    async def debug_command(self, req: DebugCommandRequest) -> EmptyReturn:
        # translate the command with the context.
        translated_command = self.translate_debug_command(req.cmd, self.context)

        # send the translated command to debugger to consume later.
        self.context.debug_manager.put_debug_command(translated_command)

        # allow MainLoop to switch into DataProcessor.
        self.context.pause_manager.resume(PauseType.USER_PAUSE)
        self.context.pause_manager.resume(PauseType.EXCEPTION_PAUSE)
        self.context.pause_manager.resume(PauseType.DEBUG_PAUSE)
        return EmptyReturn()

    @staticmethod
    def translate_debug_command(command: str, context: Context) -> str:
        """
        Cleans up and translates a debug command into one pdb can consume.

        For `b`/`break` with a numeric line target, the operator's UDF module
        name is prepended so the breakpoint lands inside the user's code:
        ``b 5`` becomes ``b my_udf:5``.

        Three forms are passed through unchanged because pdb already accepts
        them and the module rewrite would corrupt them:

        - bare ``b`` / ``break`` with no args
        - ``b <function_name>`` (pdb resolves the symbol itself)
        - ``b <filename>:<lineno>`` (the user already specified a file)

        :raises ValueError: if the command is empty/whitespace-only, or if a
            ``b``/``break`` with a numeric target is issued before the
            operator module has been initialized.
        """
        parts = command.strip().split()
        if not parts:
            raise ValueError("debug command cannot be empty")
        debug_command, *debug_args = parts

        is_break_with_lineno = (
            debug_command in ("b", "break") and debug_args and debug_args[0].isdigit()
        )
        if is_break_with_lineno:
            module_name = context.executor_manager.operator_module_name
            if module_name is None:
                raise ValueError(
                    "executor module not initialized; cannot set breakpoint"
                )
            translated = (
                f"{debug_command} {module_name}:{debug_args[0]} "
                f"{' '.join(debug_args[1:])}"
            )
        else:
            translated = f"{debug_command} {' '.join(debug_args)}"

        return translated.strip()
