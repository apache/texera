import sys
from bdb import Breakpoint

from pdb import Pdb
from threading import Condition

from typing import List, Optional, Dict

from core.architecture.managers.operator_manager import OperatorManager
from loguru import logger

from core.models import Tuple
from core.models.single_blocking_io import SingleBlockingIO


class DebugManager:
    DEBUGGER = None
    PAUSE_ON_HITTING_BREAKPOINT = False
    PAUSE_ON_SETTING_BREAKPOINT = False
    OP1_ENABLED = False
    OP2_ENABLED = False

    def __init__(self, condition: Condition, operator_manager: OperatorManager):
        self.trace_disabled = False
        self.pulled_conditions = dict()
        self._debug_in = SingleBlockingIO(condition)
        self._debug_out = SingleBlockingIO(condition)
        self._operator_manager = operator_manager
        DebugManager.DEBUGGER = Pdb(
            stdin=self._debug_in, stdout=self._debug_out, nosigint=True
        )
        self.debugger = DebugManager.DEBUGGER

        # Customized prompt, we can design our prompt for the debugger.
        self.debugger.prompt = ""
        self.breakpoints_managed = set()
        self.line_mapping = dict()
        self.states = dict()
        self.breaks_backup = dict()

    def has_debug_command(self) -> bool:
        return self._debug_in.value is not None

    def has_debug_event(self) -> bool:
        return self._debug_out.value is not None

    def get_debug_event(self) -> str:
        """
        Blocking gets for the next debug event.
        :return str: the fetched event, in string format.
        """
        event = self._debug_out.readline()
        logger.info(f"received a debug event {repr(event)}")
        return event

    def put_debug_command(self, command: str) -> None:
        """
        Puts a debug command.
        :param command: the command to be put, in string format.
        :return:
        """
        logger.info(f"put a debug command {repr(command)}")
        self._debug_in.write(command)
        self._debug_in.flush()

    def check_and_swap_for_static_breakpoints(self):
        if self.debugger.breaks:
            # there are dynamic breakpoints, do the swap
            code = None
            for ref, bps in list(Breakpoint.bplist.items()):
                for bp in bps:
                    code = self._operator_manager.add_breakpoint(bp)
                    self.breakpoints_managed.add(bp)
                    # TODO: change line mapping

            self.debugger.clear_all_breaks()
            self.disable_tracing()

            logger.info(self.debugger.breaks)
            self._operator_manager.add_operator_with_bp(
                code, is_source=self._operator_manager._operator.is_source
            )

    def disable_unnecessary_breakpoints(self, tuple_):
        bp_nums = [b.number for b in Breakpoint.bpbynumber if b is not None]
        for bp_num in bp_nums:
            if bp_num not in self.pulled_conditions:
                conditions = self.debugger.get_bpbynumber(bp_num).cond.split("and")
                data_conditions = list()
                local_conditions = list()
                for cond in conditions:
                    # recognizing the data 'tuple_' is a simple way to identify a data
                    # condition
                    if "tuple_" in cond:
                        data_conditions.append(cond)
                    else:
                        local_conditions.append(cond)
                data_condition = " and ".join(data_conditions)
                local_condition = " and ".join(local_conditions)
                logger.info(f"recorded data condition: {data_condition}")
                logger.info(f"recorded local condition: {local_condition}")
                self.pulled_conditions[bp_num] = data_condition
                self.debugger.get_bpbynumber(bp_num).cond = local_condition

        self.check_data_conditions(tuple_)

    def check_data_conditions(self, tuple_):
        disabled_bps = list()
        for id, cond in self.pulled_conditions.items():
            if eval(cond, {"tuple_": tuple_}):
                self.debugger.get_bpbynumber(id).enable()
                logger.debug(f"enabled Bp({id})")
            else:
                self.debugger.get_bpbynumber(id).disable()
                disabled_bps.append(id)
                logger.debug(f"disabled Bp({id})")
        bp_nums = [b.number for b in Breakpoint.bpbynumber if b is not None]

        if len(disabled_bps) == len(bp_nums):
            # all breakpoints are disabled, can disable debugger by removing tracing
            logger.debug("disabling debugger for current tuple")
            if self.trace_disabled:
                return

            self.disable_tracing(self.breaks_backup)
            self.debugger.breaks = []

        else:  # add tracing back
            self.debugger.breaks = self.breaks_backup
            self.enable_tracing()

    def optimize_debugger(self, current_tuple: Tuple):
        if DebugManager.OP1_ENABLED:
            self.disable_unnecessary_breakpoints(current_tuple)
        if DebugManager.OP2_ENABLED:
            self.check_and_swap_for_static_breakpoints()

    def disable_tracing(
        self, preserved_breakpoints: Optional[Dict[str, Breakpoint]] = None
    ):
        if preserved_breakpoints is not None:
            preserved_breakpoints.clear()
            preserved_breakpoints.update(self.debugger.breaks)
        self.debugger.breaks = []
        sys.settrace(None)
        frame = sys._getframe().f_back
        while frame and frame.f_back:
            del frame.f_trace
            frame = frame.f_back

    def enable_tracing(self):
        frame = sys._getframe().f_back

        while frame:
            frame.f_trace = self.debugger.trace_dispatch
            self.botframe = frame
            frame = frame.f_back
        self.trace_disabled = False

    def is_setting_breakpoint_event(self, debug_event: str) -> bool:
        return debug_event[:10] == "Breakpoint"

    def is_breakpoint_hit_event(self, debug_event: str) -> bool:
        return debug_event[:1] == ">"


def breakpoint():
    DebugManager.DEBUGGER.set_trace()
