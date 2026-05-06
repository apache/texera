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

import pytest

import core.python_worker as pw


class _FakeReceiver:
    def __init__(self, input_queue, host):
        self.input_queue = input_queue
        self.host = host
        self.proxy_server = type(
            "FakeProxyServer", (), {"get_port_number": staticmethod(lambda: 12345)}
        )()
        self._shutdown_cb = None

    def register_shutdown(self, cb):
        self._shutdown_cb = cb

    def run(self):
        pass

    def stop(self):
        pass


class _FakeSender:
    def __init__(self, output_queue, host, port, handshake_port):
        self.output_queue = output_queue
        self.host = host
        self.port = port
        self.handshake_port = handshake_port
        self.stopped = False

    def run(self):
        pass

    def stop(self):
        self.stopped = True


class _FakeMainLoop:
    def __init__(self, worker_id, input_queue, output_queue):
        self.worker_id = worker_id
        self.stopped = False

    def run(self):
        pass

    def stop(self):
        self.stopped = True


class _FakeHeartbeat:
    def __init__(self, host, port, interval, stop_event):
        self.host = host
        self.port = port
        self.interval = interval
        self.stop_event = stop_event
        self.stopped = False

    def run(self):
        pass

    def stop(self):
        self.stopped = True


@pytest.fixture
def stub_network(monkeypatch):
    monkeypatch.setattr(pw, "NetworkReceiver", _FakeReceiver)
    monkeypatch.setattr(pw, "NetworkSender", _FakeSender)
    monkeypatch.setattr(pw, "MainLoop", _FakeMainLoop)
    monkeypatch.setattr(pw, "Heartbeat", _FakeHeartbeat)


class TestPythonWorker:
    @pytest.mark.timeout(5)
    def test_construction_wires_dependencies(self, stub_network):
        worker = pw.PythonWorker(worker_id="w-1", host="localhost", output_port=9999)

        # NetworkSender must receive the handshake port from the receiver's
        # proxy server — this is the Java→Python wiring contract.
        assert worker._network_sender.handshake_port == 12345
        assert worker._network_sender.port == 9999
        # The receiver's shutdown callback is wired to worker.stop so a
        # client-side disconnect tears the worker down.
        assert worker._network_receiver._shutdown_cb == worker.stop

    @pytest.mark.timeout(5)
    def test_stop_cascades_to_main_loop_sender_and_heartbeat(self, stub_network):
        worker = pw.PythonWorker(worker_id="w-1", host="localhost", output_port=9999)

        worker.stop()

        assert worker._main_loop.stopped is True
        assert worker._network_sender.stopped is True
        assert worker._heartbeat.stopped is True

    @pytest.mark.timeout(5)
    def test_run_sets_stop_event_after_main_loop_returns(self, stub_network):
        worker = pw.PythonWorker(worker_id="w-1", host="localhost", output_port=9999)

        # All fakes' run() return immediately, so run() drains all threads
        # without blocking. The contract is that the heartbeat stop event
        # is set after the main loop / sender threads join, so the
        # heartbeat thread can exit cleanly.
        worker.run()

        assert worker._stop_event.is_set()
