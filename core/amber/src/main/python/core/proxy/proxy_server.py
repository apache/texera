import threading
import time
from functools import wraps
from inspect import signature
from typing import Callable, Dict, Iterator, Optional, Tuple

from loguru import logger
from overrides import overrides
from pyarrow import Table, py_buffer
from pyarrow.flight import (
    Action,
    FlightDescriptor,
    FlightServerBase,
    MetadataRecordBatchReader,
    Result,
    ServerCallContext,
)
from pyarrow.ipc import RecordBatchStreamWriter


class ProxyServer(FlightServerBase):
    """
    There are three kinds of messages supported by the ProxyServer:
    1. Data Messages.
        Data Messages are passed through the endpoint do_put. It will contain a
        command and a data batch.
        The user should provide deserializer for the command and a handler for the
        data batch.
    2. ProxyInternal Messages.
        ProxyInternal Messages are passed through the endpoint do_action. It will be
        used to control the life cycle of the ProxyServer. Some example messages:
        - heartbeat: checks if the ProxyServer is alive.
        - shutdown: shutdown the ProxyServer.
        - control: passing a Control Message.
    3. Control Messages.
        Control Messages are passed through the endpoint do_action. It will contain a
        command and a payload.
        The user should provide deserializer for both the command and the payload.
    """

    @staticmethod
    def ack(original_func: Optional[Callable] = None, msg="ack"):
        """
        Decorator for returning an ack message after the action. It is a Proxy level
        ack, only to be used by ProxyServer actions.

        Example usage:
            ```
            @ack
            def hello():
                return None
            server.register("hello", hello)
            msg = client.call("hello") # msg will be "ack"
            ```

            or
            ```
            @ack(msg="other msg")
            def hello():
                return None
            server.register("hello", hello)
            msg = client.call("hello") # msg will be "other msg"
            ```

        :param original_func: decorated function, usually is a callable to be
            registered.
        :param msg: the return message from the decorator, "ack" by default.
        :return:
        """

        def ack_decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
                return msg

            return wrapper

        if original_func:
            return ack_decorator(original_func)
        return ack_decorator

    def __init__(
        self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005
    ):
        location = f"{scheme}://{host}:{port}"
        super(ProxyServer, self).__init__(location)
        logger.debug(f"Serving on {location}")
        self.host = host

        # action name to callable map, will contain registered actions,
        # identified by action name.
        self._procedures: Dict[str, Tuple[Callable, str]] = dict()

        # register heartbeat, this is the default action for the client to
        # check the aliveness of the server.
        self.register(name="heartbeat", action=ProxyServer.ack()(lambda: None))

        # register shutdown, this is the default action for the client to
        # terminate the server.
        self.register(
            name="shutdown",
            action=ProxyServer.ack(msg="Bye bye!")(
                lambda: threading.Thread(target=self._shutdown).start()
            ),
            description="Shut down this server.",
        )

        # register control, this is the default action for the client to invoke
        # after receiving control.
        self.register(
            name="control",
            action=ProxyServer.ack()(
                lambda control_message: self.process_control(control_message)
            ),
            description="Process the control message",
        )

        # the data message handler for each data message, needs to be
        # implemented during runtime.
        self.process_data = lambda *args, **kwargs: (_ for _ in ()).throw(
            NotImplementedError
        )

        # the control message handler for each control message, needs to be
        # implemented during runtime.
        self.process_control = lambda *args, **kwargs: (_ for _ in ()).throw(
            NotImplementedError
        )

    ###########################
    # Flights related methods #
    ###########################
    @overrides(check_signature=False)
    def do_put(
        self,
        context: ServerCallContext,
        descriptor: FlightDescriptor,
        reader: MetadataRecordBatchReader,
        writer: RecordBatchStreamWriter,
    ):
        """
        Put a data table into the server, the data will be handled by the
        `self.process_data()` handler.

        :param context: server context, containing information of middlewares.
        :param descriptor: the descriptor of this batch of data.
        :param reader: the input stream of batches of records.
        :param writer: the output stream.
        :return:
        """

        data: Table = reader.read_all()
        command: bytes = descriptor.command
        logger.debug(f"getting a data batch {data}")
        self.process_data(command, data)

    ###############################
    # Actions related methods #
    ###############################
    @overrides(check_signature=False)
    def list_actions(self, context: ServerCallContext) -> Iterator[Tuple[str, str]]:
        """
        List all actions that are being registered with the server, it will
        return the action name and description for each registered action.

        :param context: server context, containing information of middlewares.
        :return: iterator of (action_name, action_description) pairs.
        """
        return map(lambda x: (x[0], x[1][1]), self._procedures.items())

    @overrides(check_signature=False)
    def do_action(self, context: ServerCallContext, action: Action) -> Iterator[Result]:
        """
        Perform an action that previously registered with a action,
        return a result in bytes.

        :param context: server context, containing information of middlewares.
        :param action: the action to perform, including
                        action.type: the action name to invoke
                        action.body: the action arguments in bytes
        :return: yield the encoded result back to client.
        """

        action_name = action.type
        logger.debug(f"python getting a call on {action_name}")
        # get action by name
        if action_name in self._procedures:
            procedure, _ = self._procedures.get(action_name)
            if not action:
                raise KeyError("Unknown action {!r}".format(action_name))

            payload = action.body.to_pybytes()
            # invoke the action
            if payload:
                result = procedure(payload)
            else:
                result = procedure()

            # serialize the result
            if isinstance(result, bytes):
                encoded = result
            else:
                encoded = str(result).encode("utf-8")
        else:
            raise KeyError("Unknown action {!r}".format(action_name))
        yield Result(py_buffer(encoded))

    @logger.catch(reraise=True)
    def register(self, name: str, action: Callable, description: str = "") -> None:
        """
        Register an action with the action name.

        :param name: the name of the action, it should be matching Action's type.
        :param action: a callable, could be class, function, or lambda.
        :param description: describes the action.
        :return:
        """

        # wrap the given action so that its error can be logged.
        @logger.catch(level="WARNING", reraise=True)
        def wrapper(*args, **kwargs):
            return action(*args, **kwargs)

        # update the actions, which overwrites the previous registration.
        self._procedures[name] = (wrapper, description)
        logger.debug(f"registered action {name}")

    @logger.catch(reraise=True)
    def register_data_handler(self, handler: Callable) -> None:
        """
        Register the data handler function, which will be invoked after each `do_put`.

        :param handler: a callable with at least two arguments, for
            1) the command and 2) the data batch.
        :return:
        """

        # the handler should have at least 2 arguments
        assert len(signature(handler).parameters) >= 2
        self.process_data = handler

    @logger.catch(reraise=True)
    def register_control_handler(self, handler: Callable) -> None:
        """
        Register a control handler function, which will be invoked after each
        `do_action` with `control` as the command.

        :param handler: a callable with at least two arguments, for 1) the command
            and 2) the control payload.
        :return:
        """
        # the handler should have at least 1 argument
        assert len(signature(handler).parameters) >= 1
        self.process_control = handler

    ##################
    # helper methods #
    ##################
    def _shutdown(self):
        """Shut down after a delay."""
        logger.debug("Server is shutting down...")
        time.sleep(1)
        self.shutdown()
