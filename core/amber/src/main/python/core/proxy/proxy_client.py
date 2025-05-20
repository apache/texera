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

from loguru import logger
from pyarrow import Table, Buffer
from pyarrow.flight import (
    Action,
    FlightCallOptions,
    FlightClient,
    FlightDescriptor,
    FlightStreamWriter,
    FlightMetadataReader,
)
from typing import Optional


class ProxyClient(FlightClient):
    def __init__(
        self,
        scheme: str = "grpc+tcp",
        host: str = "localhost",
        port: int = 5005,
        handshake_port: Optional[int] = None,
        timeout=1000,
        *args,
        **kwargs,
    ):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.debug(f"Connected to server at {location}")
        self._timeout = timeout
        if handshake_port is not None:
            self._handshake(handshake_port=handshake_port)

    @logger.catch(reraise=True)
    def call_action(
        self,
        action_name: str,
        payload: bytes = bytes(),
        options: Optional[FlightCallOptions] = None,
    ) -> bytes:
        """
        Call a specific remote action specified by the name, pass along a payload.
        :param action_name: the registered action name to be invoked.
        :param payload: the action payload in bytes, user should take the
            responsibility to deserialize it.
        :param options: FlightCallOption to config the call.
        :return: exactly one result in bytes.
        """

        action = Action(action_name, payload)
        if options is None:
            options = FlightCallOptions(timeout=self._timeout)

        # Arrow allows multiple results from the Action call return as a stream (
        # interator). In Arrow 11, it alerts if the results are not consumed fully.
        # As we do our own Async RPC management, we are currently not using results
        # from Action call. In the future, this results can include credits for flow
        # control purpose.
        results = list(self.do_action(action, options))

        # However, we will only expect exactly one result for now.
        assert len(results) == 1

        return results[0].body.to_pybytes()

    @logger.catch(reraise=True)
    def send_data(self, command: bytes, table: Optional[Table]) -> int:
        """
        Send a data batch to the server.
        :param command: a command to in descriptor to pass along, user should take
            the responsibility to deserialize it.
        :param table: a PyArrow.Table of column-stored records.
        :return: an integer representing credit values received from ack
        """
        descriptor = FlightDescriptor.for_command(command)
        table = Table.from_arrays([]) if table is None else table
        writer, reader = self.do_put(descriptor, table.schema)
        writer: FlightStreamWriter
        reader: FlightMetadataReader
        with writer:
            writer.write_table(table)
            credit_buf: Buffer = reader.read()
            credit_count: int = int.from_bytes(
                credit_buf.to_pybytes(), byteorder="little"
            )
        return credit_count

    def _handshake(self, handshake_port: int) -> None:
        """
        Send the handshake port to Java Proxy Server, which will be forwarded to
        the Java Proxy Client to use.
        :param handshake_port: int, the port number for Java Proxy Client to connect
        to Python Proxy Server.
        :return:
        """
        self.call_action("handshake", bytes(str(handshake_port), "utf-8"))
