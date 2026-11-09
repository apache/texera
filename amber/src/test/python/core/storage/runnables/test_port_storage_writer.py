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

from unittest.mock import MagicMock

import pytest

from core.util.customized_queue.queue_base import QueueControl, QueueElement
from core.storage.runnables.port_storage_writer import (
    PortStorageWriter,
    PortStorageWriterElement,
)


@pytest.fixture
def writer_mock():
    return MagicMock()


@pytest.fixture
def writer(writer_mock):
    return PortStorageWriter(buffered_item_writer=writer_mock, queue=MagicMock())


class TestReceive:
    def test_dispatches_data_tuple_to_buffered_item_writer(self, writer, writer_mock):
        data_tuple = object()
        element = PortStorageWriterElement(data_tuple=data_tuple)

        writer.receive(element)

        # The wrapped tuple is forwarded verbatim to the underlying writer.
        writer_mock.put_one.assert_called_once_with(data_tuple)

    def test_raises_type_error_on_a_plain_queue_element(self, writer, writer_mock):
        with pytest.raises(TypeError):
            writer.receive(QueueElement())

        # A non-data element must never be written out.
        writer_mock.put_one.assert_not_called()

    def test_raises_type_error_on_a_control_message(self, writer, writer_mock):
        with pytest.raises(TypeError):
            writer.receive(QueueControl(msg="stop"))

        writer_mock.put_one.assert_not_called()


class TestLifecycle:
    def test_pre_start_opens_the_writer(self, writer, writer_mock):
        writer.pre_start()

        writer_mock.open.assert_called_once_with()
        writer_mock.close.assert_not_called()

    def test_post_stop_closes_the_writer(self, writer, writer_mock):
        writer.post_stop()

        writer_mock.close.assert_called_once_with()
        writer_mock.open.assert_not_called()
