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


class DataProcessingConfig:
    """
    Configuration for data processing behavior.
    Initialized from command-line args passed from Scala.
    """

    disable_control_message_checking: bool = False
    output_batch_size: int = 1

    @classmethod
    def initialize(
        cls, disable_control_message_checking: bool, output_batch_size: int = 1
    ) -> None:
        """
        Initialize the data processing configuration.

        :param disable_control_message_checking: When True, skips control message
            checking during tuple processing for better throughput.
        :param output_batch_size: Number of output tuples to batch before context
            switching. Higher values reduce context switch overhead but increase
            latency. Default is 1 (original behavior).
        """
        cls.disable_control_message_checking = disable_control_message_checking
        cls.output_batch_size = max(1, output_batch_size)
