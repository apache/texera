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
from pyarrow.flight import Action

from .proxy_server import ProxyServer


class TestProxyServer:
    @pytest.fixture()
    def server(self):
        server = ProxyServer()
        yield server
        server.graceful_shutdown()

    def test_server_can_register_control_actions_with_lambda(self, server):
        assert "hello" not in server._procedures
        server.register("hello", lambda: None)
        assert "hello" in server._procedures

    def test_server_can_register_control_actions_with_function(self, server):
        def hello():
            return None

        assert "hello" not in server._procedures
        server.register("hello", hello)
        assert "hello" in server._procedures

    def test_server_can_register_control_actions_with_callable_class(self, server):
        class Hello:
            def __call__(self):
                return None

        assert "hello" not in server._procedures
        server.register("hello", Hello())
        assert "hello" in server._procedures

    def test_server_can_invoke_registered_control_actions(self, server):
        procedure_contents = {
            "hello": "hello world",
            "get an int": 12,
            "get a float": 1.23,
            "get a tuple": (5, None, 123.4),
            "get a list": [5, (None, 123.4)],
            "get a dict": {"entry": [5, (None, 123.4)]},
        }

        for name, result in procedure_contents.items():
            server.register(name, lambda: result)
            assert name in server._procedures
            assert next(
                server.do_action(None, Action(name, b""))
            ).body.to_pybytes() == str(result).encode("utf-8")
