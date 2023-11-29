from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import SchedulerTimeSlotEventV2


class SchedulerTimeSlotEventHandler(ControlHandler):
    cmd = SchedulerTimeSlotEventV2

    def __call__(
        self, context: Context, command: SchedulerTimeSlotEventV2, *args, **kwargs
    ):
        context.main_loop._scheduler_time_slot_event(command.time_slot_expired)
