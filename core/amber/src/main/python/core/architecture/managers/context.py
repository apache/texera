from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState
from .console_message_manager import ConsoleMessageManager
from .debug_manager import DebugManager
from .exception_manager import ExceptionManager
from .tuple_processing_manager import TupleProcessingManager
from .executor_manager import ExecutorManager
from .pause_manager import PauseManager
from .state_manager import StateManager
from .statistics_manager import StatisticsManager
from ..packaging.input_manager import InputManager
from ..packaging.output_manager import OutputManager
from ...models import InternalQueue


class Context:
    """
    Manages context of command handlers. Many of those attributes belongs to the DP
    thread, they are managed here to show a clean interface what handlers can or
    should access.

    Context class can be viewed as a friend of DataProcessor.
    """

    def __init__(self, worker_id, input_queue):
        self.worker_id = worker_id
        self.input_queue: InternalQueue = input_queue
        self.executor_manager = ExecutorManager()
        self.tuple_processing_manager = TupleProcessingManager()
        self.exception_manager = ExceptionManager()
        self.state_manager = StateManager(
            {
                WorkerState.UNINITIALIZED: {WorkerState.READY},
                WorkerState.READY: {WorkerState.PAUSED, WorkerState.RUNNING},
                WorkerState.RUNNING: {WorkerState.PAUSED, WorkerState.COMPLETED},
                WorkerState.PAUSED: {WorkerState.RUNNING},
                WorkerState.COMPLETED: set(),
            },
            WorkerState.UNINITIALIZED,
        )

        self.statistics_manager = StatisticsManager()
        self.pause_manager = PauseManager(
            self.input_queue, state_manager=self.state_manager
        )
        self.output_manager = OutputManager()
        self.input_manager = InputManager()
        self.console_message_manager = ConsoleMessageManager()
        self.debug_manager = DebugManager(
            self.tuple_processing_manager.context_switch_condition
        )

    def close(self):
        self.executor_manager.close()
