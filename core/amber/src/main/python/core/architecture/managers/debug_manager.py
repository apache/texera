from pdb import Pdb
from threading import Condition

from core.models.single_blocking_io import SingleBlockingIO


class DebugManager:
    def __init__(self, condition: Condition):
        self._debug_in = SingleBlockingIO(condition)
        self._debug_out = SingleBlockingIO(condition)
        self.debugger = Pdb(stdin=self._debug_in, stdout=self._debug_out, nosigint=True)

        # Customized prompt, we can design our prompt for the debugger.
        self.debugger.prompt = ""

    def has_debug_command(self) -> bool:
        return self._debug_in.value is not None

    def has_debug_event(self) -> bool:
        return self._debug_out.value is not None

    def get_debug_event(self) -> str:
        """
        Blocking gets for the next debug event.
        :return str: the fetched event, in string format.
        """
        return self._debug_out.readline()

    def put_debug_command(self, command: str) -> None:
        """
        Puts a debug command.
        :param command: the command to be put, in string format.
        :return:
        """
        self._debug_in.write(command)
        self._debug_in.flush()
