from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
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
        This method cleans up, reformats, and then translates a debug command into
        a command that can be understood by the debugger.

        For example, it adds the UDF code context.

        :param command:
        :param context:
        :return:
        """
        debug_command, *debug_args = command.strip().split()
        module_name = context.executor_manager.operator_module_name
        if debug_command in ["b", "break"] and len(debug_args) > 0:
            # b(reak) ([filename:]lineno | function) [, condition]¶
            translated_command = (
                f"{debug_command} {module_name}:{debug_args[0]} "
                f"{' '.join(debug_args[1:])}"
            )
        else:
            translated_command = f"{debug_command} {' '.join(debug_args)}"

        translated_command = translated_command.strip()
        return translated_command
