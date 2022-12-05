from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerDebugCommandV2,
)
from .handler_base import Handler
from ..managers.context import Context


class WorkerDebugCommandHandler(Handler):
    cmd = WorkerDebugCommandV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        logger.info(f"Got WorkerDebugCommand {command}")

        # re-format the command with the context
        debug_command, *debug_args = command.cmd.split()
        module_name = context.operator_manager.operator_module_name
        if debug_command in ["b", "break"] and len(debug_args) > 0:
            # b(reak) ([filename:]lineno | function) [, condition]¶
            formatted_command = f"{debug_command} {module_name}:{debug_args[0]}"
        else:
            formatted_command = f"{debug_command} {' '.join(debug_args)}".strip()
        logger.info("formatted debug command " + formatted_command)

        if not context.debug_manager.talk_with_debugger:

            # initialize debugger
            logger.info("sending an emtpy string to invoke pdb")
            context.debug_manager.debug_in.write("")
            context.debug_manager.debug_in.flush()

            # ignore the initial message sent by pdb.
            context.debug_manager.get_debug_event()
            logger.info("pdb is up, ready to send command")

            # send the actual command in
            context.debug_manager.debug_in.write(formatted_command)
            context.debug_manager.debug_in.flush()

            return None
        else:
            context.main_loop._resume_dp()

            context.debug_manager.debug_in.write(formatted_command)
            context.debug_manager.debug_in.flush()
