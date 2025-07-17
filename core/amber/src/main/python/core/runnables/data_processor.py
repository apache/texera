# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys
import traceback
from loguru import logger
from threading import Event
from typing import Iterator, Optional

from core.architecture.managers import Context
from core.models import ExceptionInfo, State, TupleLike
from core.models.internal_marker import StartOfInputPort, EndOfInputPort, EndOfInputPorts
from core.models.marker import Marker
from core.models.table import all_output_to_tuple
from core.util import Stoppable
from core.util.console_message.replace_print import replace_print
from core.util.console_message.timestamp import current_time_in_local_timezone
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ConsoleMessageType,
)


class DataProcessor(Runnable, Stoppable):
    def __init__(self, context: Context):
        self._running = Event()
        self._context = context

    def run(self) -> None:
        """
        Start the data processing loop. Wait for context switch conditions to be met,
        then continuously process markers or tuples until stopped.
        """
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.tuple_processing_manager.context_switch_condition.wait()
        self._running.set()
        self._switch_context()
        while self._running.is_set():
            marker = self._context.marker_processing_manager.get_input_marker()
            tuple_ = self._context.tuple_processing_manager.current_input_tuple
            if marker is not None:
                self.process_marker(marker)
            elif tuple_ is not None:
                self.process_tuple()
            else:
                raise RuntimeError("No marker or tuple to process.")
            self._switch_context()

    def process_marker(self, marker: Marker) -> None:
        """
        Process an input marker by invoking appropriate state
        or tuple generation based on the marker type.
        """
        try:
            executor = self._context.executor_manager.executor
            port_id = self._context.tuple_processing_manager.get_input_port_id()
            with replace_print(
                    self._context.worker_id,
                    self._context.console_message_manager.print_buf,
            ):
                if isinstance(marker, StartOfInputPort):
                    method_name = f'on_start_{marker.port_id}'

                    # Check if the executor has this method
                    process_method = getattr(executor, method_name, None)
                    if callable(process_method):
                        process_method()
                    else:
                        logger.info(
                            f"open for port {marker.port_id} not found, skipped.")
                    self._set_output_state(executor.produce_state_on_start(port_id))
                elif isinstance(marker, State):
                    self._set_output_state(executor.process_state(marker, port_id))
                elif isinstance(marker, EndOfInputPort):
                    self._set_output_state(executor.produce_state_on_finish(port_id))
                    self._switch_context()
                    if self._context.input_manager.is_source:
                        if hasattr(executor, "produce"):
                            self._set_output_tuple(executor.produce())
                    else:
                        method_name = "process_table_" + str(port_id)
                        if hasattr(executor, method_name):
                            from core.models.table import Table
                            table = Table(executor.TABLE_DATA_INTERNAL[port_id])
                            method_name = 'process_table_' + str(port_id)
                            process_method = getattr(executor, method_name, None)
                            self._set_output_tuple(process_method(table))
                        else:
                            method_name = f'on_finish_{port_id}'
                            # Check if the executor has this method
                            process_method = getattr(executor, method_name, None)
                            if callable(process_method):
                                it = process_method()
                                self._set_output_tuple(it)
                            else:
                                logger.info(f"on_finish for port {port_id} not "
                                            f"found, skipped.")
                                self._set_output_tuple([])
                elif isinstance(marker, EndOfInputPorts):
                    # End of all input ports, finalize the processing.
                    self._set_output_tuple(executor.on_finish_all())


        except Exception as err:
            logger.exception(err)
            exc_info = sys.exc_info()
            self._context.exception_manager.set_exception_info(exc_info)
            self._report_exception(exc_info)

        finally:
            self._switch_context()

    def process_tuple(self) -> None:
        """
        Process an input tuple by invoking the executor's tuple processing method.
        """
        finished_current = self._context.tuple_processing_manager.finished_current
        while not finished_current.is_set():
            try:
                executor = self._context.executor_manager.executor
                port_id = self._context.tuple_processing_manager.get_input_port_id()
                tuple_ = self._context.tuple_processing_manager.get_input_tuple()
                with replace_print(
                        self._context.worker_id,
                        self._context.console_message_manager.print_buf,
                ):
                    method_name = f'process_tuple_{port_id}'
                    # Check if the executor has this method
                    process_method = getattr(executor, method_name, None)
                    if callable(process_method):
                        it = process_method(tuple_)
                        self._set_output_tuple(it)
                    else:
                        # table api
                        self._set_output_tuple(executor.collect(tuple_, port_id))

            except Exception as err:
                logger.exception(err)
                exc_info = sys.exc_info()
                self._context.exception_manager.set_exception_info(exc_info)
                self._report_exception(exc_info)

            finally:
                self._switch_context()

    def _set_output_tuple(self, output_iterator: Iterator[Optional[TupleLike]]) -> None:
        """
        Set the output tuple after processing by the executor.
        """
        self._context.tuple_processing_manager.finished_current.clear()
        for output in output_iterator:
            from typing import Tuple
            if (isinstance(output, Tuple) and len(output) == 2 and isinstance(output[
                                                                                  1], int)):
                real_output = output[0]
                output_port = output[1]
            # output could be a None, a TupleLike, or a TableLike.
            else:
                real_output = output
                output_port = 0
            for output_tuple in all_output_to_tuple(real_output):
                logger.info("Output tuple: " + str(output_tuple))
                if output_tuple is not None:
                    output_tuple.finalize(
                        self._context.output_manager.get_port(output_port).get_schema()
                    )
                self._switch_context()
                self._context.tuple_processing_manager.current_output_tuple = (
                    output_tuple
                )
                self._context.tuple_processing_manager.current_output_port_id = output_port
                self._switch_context()
        self._context.tuple_processing_manager.finished_current.set()

    def _set_output_state(self, output_state: State) -> None:
        """
        Set the output state after processing by the executor.
        """
        self._context.marker_processing_manager.current_output_state = output_state

    def _switch_context(self) -> None:
        """
        Notify the MainLoop thread and wait here until being switched back.
        """
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.tuple_processing_manager.context_switch_condition.notify()
            self._context.tuple_processing_manager.context_switch_condition.wait()
        self._post_switch_context_checks()

    def _check_and_process_debug_command(self) -> None:
        """
        If a debug command is available, invokes the debugger from this frame.
        """
        if self._context.debug_manager.has_debug_command():
            # Let debugger trace from the current frame.
            # This line will also trigger cmdloop in the debugger.
            # This line has no side effects on the current debugger state.
            self._context.debug_manager.debugger.set_trace()

    def _post_switch_context_checks(self):
        self._check_and_process_debug_command()

    def _report_exception(self, exc_info: ExceptionInfo):
        tb = traceback.extract_tb(exc_info[2])
        filename, line_number, func_name, text = tb[-1]
        base_name = os.path.basename(filename)
        module_name, _ = os.path.splitext(base_name)
        formatted_exception = traceback.format_exception(*exc_info)
        title: str = formatted_exception[-1].strip()
        message: str = "\n".join(formatted_exception)

        self._context.console_message_manager.put_message(
            ConsoleMessage(
                worker_id=self._context.worker_id,
                timestamp=current_time_in_local_timezone(),
                msg_type=ConsoleMessageType.ERROR,
                source=f"{module_name}:{func_name}:{line_number}",
                title=title,
                message=message,
            )
        )

    def stop(self):
        self._running.clear()
