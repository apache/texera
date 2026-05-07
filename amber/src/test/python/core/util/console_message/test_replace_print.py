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

import builtins
import io
from typing import List

import pytest

from core.util.console_message.replace_print import replace_print
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ConsoleMessageType,
)


class CapturingBuffer:
    """Minimal IBuffer stand-in that just records put calls."""

    def __init__(self):
        self.messages: List[ConsoleMessage] = []

    def put(self, msg):
        self.messages.append(msg)


class TestReplacePrintLifecycle:
    def test_print_is_replaced_inside_the_context_and_restored_on_exit(self):
        original = builtins.print
        buf = CapturingBuffer()
        with replace_print("w", buf):
            assert builtins.print is not original
        assert builtins.print is original

    def test_print_is_restored_even_when_the_block_raises(self):
        original = builtins.print
        buf = CapturingBuffer()
        with pytest.raises(RuntimeError):
            with replace_print("w", buf):
                raise RuntimeError("boom")
        assert builtins.print is original

    def test_exit_returns_true_for_clean_block_and_false_for_raising_block(self):
        # Pin: __exit__ returns True when no exception, False otherwise. The
        # contextlib protocol then suppresses or surfaces the exception
        # accordingly. The class returns False on exception, so the exception
        # propagates out — matching the docstring claim.
        ctx = replace_print("w", CapturingBuffer())
        ctx.__enter__()
        assert ctx.__exit__(None, None, None) is True
        ctx2 = replace_print("w", CapturingBuffer())
        ctx2.__enter__()
        try:
            assert ctx2.__exit__(RuntimeError, RuntimeError("x"), None) is False
        finally:
            # The class only restores `print` if __exit__ runs to completion;
            # call it explicitly to clean up either way.
            builtins.print = ctx2.builtins_print


class TestReplacePrintBufferPayload:
    def test_print_inside_context_enqueues_a_console_message(self):
        buf = CapturingBuffer()
        with replace_print("worker-A", buf):
            print("hello")
        assert len(buf.messages) == 1
        msg = buf.messages[0]
        assert msg.worker_id == "worker-A"
        assert msg.msg_type == ConsoleMessageType.PRINT
        # Default print appends a newline; the title carries the full line.
        assert msg.title == "hello\n"
        assert msg.message == ""

    def test_joins_args_via_the_real_print_so_sep_and_end_kwargs_apply(self):
        buf = CapturingBuffer()
        with replace_print("w", buf):
            print("a", "b", "c", sep="-", end="!")
        assert buf.messages[0].title == "a-b-c!"

    def test_each_print_call_produces_one_buffer_entry(self):
        # Pin: the wrapped print writes to the buffer once per print call,
        # not once per argument (contextlib.redirect_stdout-style would do the
        # latter). The docstring calls this out.
        buf = CapturingBuffer()
        with replace_print("w", buf):
            print("first")
            print("second", "third")
        assert [m.title for m in buf.messages] == ["first\n", "second third\n"]

    def test_print_with_file_kwarg_bypasses_the_buffer(self):
        # When the caller provides a `file=...` argument, the wrap delegates
        # straight to the original builtins.print and does not enqueue a
        # ConsoleMessage. This is what lets explicit logging redirects keep
        # working inside the context.
        buf = CapturingBuffer()
        sink = io.StringIO()
        with replace_print("w", buf):
            print("ignored-by-buffer", file=sink)
        assert buf.messages == []
        assert sink.getvalue() == "ignored-by-buffer\n"

    def test_source_field_records_caller_module_function_and_line(self):
        # The wrap walks one frame up to identify where the print() came from,
        # so the source string carries `<module>:<func>:<lineno>`. We verify
        # only the structural parts — the exact line number and module name
        # depend on this test's location, so use loose checks.
        buf = CapturingBuffer()

        def caller_under_test():
            print("from-caller")

        with replace_print("w", buf):
            caller_under_test()

        source = buf.messages[0].source
        parts = source.split(":")
        assert len(parts) == 3
        # The reported function name is the function that called print().
        assert parts[1] == "caller_under_test"
        # And the line number is a positive integer.
        assert parts[2].isdigit() and int(parts[2]) > 0
