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

from abc import abstractmethod
from typing import Iterator, Optional, List, Dict, Set

from pyamber import *


class UDFMultiTableOperator(TableOperator):
    """
    Base class for multi-table user-defined operators that process multiple
    input tables together. Input tables are automatically stored as named
    attributes based on INPUT_PORTS declaration.

    Subclasses must:
    1. Define INPUT_PORTS class variable with port names (e.g., ["products", "merchants"])
    2. Implement process_tables() method

    Example:
        class ProcessTablesOperator(UDFMultiTableOperator):
            INPUT_PORTS = ["products", "merchants"]

            def process_tables(self) -> Iterator[Optional[TableLike]]:
                # Access tables as self.products, self.merchants
                merged = self.products.merge(self.merchants, on='id')
                yield merged
    """

    # Override in subclass to declare input port names
    # Port names will become attributes: self.products, self.merchants, etc.
    INPUT_PORTS: List[str] = []

    def __init__(self):
        super().__init__()
        self._port_tables: Dict[int, Table] = {}
        self._completed_ports: Set[int] = set()

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating
        some resources, such as a file, a model, or an API client.
        """
        pass

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Internal method that collects tables from each port.
        When all ports have completed, assigns tables to named attributes
        and calls process_tables().

        Do not override this method - implement process_tables() instead.
        """
        # Store the table for this port
        self._port_tables[port] = table
        self._completed_ports.add(port)

        # Check if all declared ports have completed
        num_expected_ports = len(self.INPUT_PORTS) if self.INPUT_PORTS else 1

        if len(self._completed_ports) < num_expected_ports:
            # Not all ports complete yet, don't emit anything
            # Just return early - this produces an empty iterator
            return

        # All ports have completed - assign tables to named attributes
        if self.INPUT_PORTS:
            for i, port_name in enumerate(self.INPUT_PORTS):
                if i in self._port_tables:
                    setattr(self, port_name, self._port_tables[i])
                else:
                    # Port not received, set to None
                    setattr(self, port_name, None)

        # Call user's process_tables implementation
        yield from self.process_tables()

    @abstractmethod
    def process_tables(self) -> Iterator[Optional[TableLike]]:
        """
        Process all input tables together. Override this method in your subclass.

        All input tables are available as named attributes based on INPUT_PORTS.
        For example, if INPUT_PORTS = ["products", "merchants"], then:
        - self.products contains the DataFrame from port 0
        - self.merchants contains the DataFrame from port 1

        :return: Iterator[Optional[TableLike]], producing one TableLike object
            at a time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
