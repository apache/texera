from proto.edu.uci.ics.amber.engine.architecture.rpc import WorkerServiceBase


class ControlHandler(WorkerServiceBase):
    def __init__(self, context):
        self.context = context
