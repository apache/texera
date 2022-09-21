from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import SchedulerTimeSlotEventV2


class SchedulerTimeSlotEventHandler(Handler):
    cmd = SchedulerTimeSlotEventV2

    def __call__(
        self, context: Context, command: SchedulerTimeSlotEventV2, *args, **kwargs
    ):
        context.dp._scheduler_time_slot_event(command.time_slot_expired)
