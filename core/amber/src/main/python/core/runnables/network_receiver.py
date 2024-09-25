from loguru import logger
from overrides import overrides
from pyarrow.lib import Table
from typing import Optional
from pampy import match

from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)
from core.architecture.handlers.actorcommand.backpressure_handler import (
    BackpressureHandler,
)
from core.architecture.handlers.actorcommand.credit_update_handler import (
    CreditUpdateHandler,
)
from core.models import (
    DataFrame,
    MarkerFrame,
)
from core.models.internal_queue import DataElement, ControlElement, InternalQueue
from core.models.marker import EndOfInputChannel, State, StartOfInputChannel
from core.proxy import ProxyServer
from core.util import Stoppable, get_one_of
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.common import (
    PythonControlMessage,
    PythonDataHeader,
    PythonActorMessage,
    ActorCommand,
)


class NetworkReceiver(Runnable, Stoppable):
    """
    Receive and deserialize messages.
    """

    @logger.catch(reraise=True)
    def __init__(
        self, shared_queue: InternalQueue, host: str, port: Optional[int] = None
    ):
        server_start = False
        # try to start the server until it succeeds
        while not server_start:
            try:
                self._proxy_server = ProxyServer(host=host, port=port)
                server_start = True
            except Exception as e:
                logger.debug("Error occurred while starting the server:", repr(e))

        self._handlers: dict[type(ActorCommand), ActorCommandHandler] = dict()

        self.register_actor_command_handler(BackpressureHandler())
        self.register_actor_command_handler(CreditUpdateHandler())

        # register the data handler to deserialize data messages.
        @logger.catch(reraise=True)
        def data_handler(command: bytes, table: Table) -> int:
            """
            Data handler for deserializing data messages

            :param command:
            :param table:
            :return: sender credits
            """
            data_header = PythonDataHeader().parse(command)
            payload = match(
                data_header.payload_type,
                "Data",
                lambda _: DataFrame(table),
                "State",
                lambda _: MarkerFrame(State(table)),
                "StartOfInputChannel",
                MarkerFrame(StartOfInputChannel()),
                "EndOfInputChannel",
                MarkerFrame(EndOfInputChannel()),
            )

            shared_queue.put(DataElement(tag=data_header.tag, payload=payload))
            return shared_queue.in_mem_size()

        self._proxy_server.register_data_handler(data_handler)

        @logger.catch(reraise=True)
        def control_handler(message: bytes) -> int:
            """
            Control handler for deserializing control messages

            :param message:
            :return: sender credits
            """
            python_control_message = PythonControlMessage().parse(message)
            shared_queue.put(
                ControlElement(
                    tag=python_control_message.tag,
                    payload=python_control_message.payload,
                )
            )
            return shared_queue.in_mem_size()

        self._proxy_server.register_control_handler(control_handler)

        @logger.catch(reraise=True)
        def actor_message_handler(message: bytes) -> int:
            """
            Control handler for deserializing actor messages

            :param message:
            :return: sender credits
            """
            python_actor_message = PythonActorMessage().parse(message)
            command = get_one_of(python_actor_message.payload)
            self.look_up(command)(command, shared_queue)
            return shared_queue.in_mem_size()

        self._proxy_server.register_actor_message_handler(actor_message_handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._proxy_server.register(
            name="shutdown", action=ProxyServer.ack(msg="Bye bye!")(shutdown)
        )

    @logger.catch(reraise=True)
    @overrides
    def run(self) -> None:
        logger.debug("started running!!!")
        self._proxy_server.serve()

    @logger.catch(reraise=True)
    @overrides
    def stop(self):
        self._proxy_server.graceful_shutdown()
        self._proxy_server.wait()

    @property
    def proxy_server(self):
        return self._proxy_server

    def register_actor_command_handler(self, handler: ActorCommandHandler) -> None:
        self._handlers[handler.cmd] = handler

    def look_up(self, cmd: ActorCommand) -> ActorCommandHandler:
        logger.debug(cmd)
        return self._handlers[type(cmd)]
