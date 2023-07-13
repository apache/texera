from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState
from .console_message_manager import ConsoleMessageManager
from .debug_manager import DebugManager
from .exception_manager import ExceptionManager
from .tuple_processing_manager import TupleProcessingManager
from .operator_manager import OperatorManager
from .pause_manager import PauseManager
from .state_manager import StateManager
from .statistics_manager import StatisticsManager
from ..packaging.batch_to_tuple_converter import BatchToTupleConverter
from ..packaging.tuple_to_batch_converter import TupleToBatchConverter


class Context:
    """
    Manages context of command handlers. Many of those attributes belongs to the DP
    thread, they are managed here to show a clean interface what handlers can or
    should access.

    Context class can be viewed as a friend of DataProcessor.
    """

    def __init__(self, worker_id, main_loop):
        self.worker_id = worker_id
        self.main_loop = main_loop
        self.input_queue = main_loop._input_queue
        self.operator_manager = OperatorManager()
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
        self.pause_manager = PauseManager()
        self.tuple_to_batch_converter = TupleToBatchConverter()
        self.batch_to_tuple_converter = BatchToTupleConverter()
        self.console_message_manager = ConsoleMessageManager()
        self.debug_manager = DebugManager(
            self.tuple_processing_manager.context_switch_condition
        )

    def close(self):
        self.operator_manager.close()
