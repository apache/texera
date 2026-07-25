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

from threading import Condition

from core.models.single_blocking_io import SingleBlockingIO


class ScriptedCondition:
    """A Condition stand-in that lets a test drive readline()'s wait loop
    deterministically, with no real thread and no sleep.

    SingleBlockingIO only uses the context-manager protocol plus notify() and
    wait(); this records those calls and runs an optional ``on_wait`` hook to
    simulate a producer flushing a value while the reader is parked.
    """

    def __init__(self):
        self.wait_calls = 0
        self.notify_calls = 0
        self.on_wait = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def notify(self, n: int = 1) -> None:
        self.notify_calls += 1

    def wait(self, timeout=None) -> bool:
        self.wait_calls += 1
        if self.on_wait is not None:
            self.on_wait()
        return True


class TestWrite:
    def test_write_appends_to_buffer_without_publishing_a_value(self):
        io = SingleBlockingIO(Condition())

        io.write("ab")
        io.write("cd")

        assert io.buf == "abcd"
        # Nothing is readable until flush().
        assert io.value is None


class TestFlush:
    def test_flush_completes_with_newline_and_moves_buffer_to_value(self):
        io = SingleBlockingIO(Condition())
        io.write("hello")

        io.flush()

        assert io.value == "hello\n"
        # The buffer is reset so the next line starts empty.
        assert io.buf == ""

    def test_flush_of_empty_buffer_publishes_just_a_newline(self):
        io = SingleBlockingIO(Condition())

        io.flush()

        assert io.value == "\n"
        assert io.buf == ""


class TestReadline:
    def test_returns_the_flushed_value_and_clears_it(self):
        # With a value already published, readline() must not block: it
        # returns immediately and clears the IO for the next line.
        io = SingleBlockingIO(Condition())
        io.write("hello")
        io.flush()

        assert io.readline() == "hello\n"
        assert io.value is None

    def test_blocks_until_a_value_appears_then_returns_and_clears(self):
        # value starts as None, so readline() must enter its wait loop. The
        # scripted condition simulates a producer that writes+flushes a line
        # on the first wait(), which unblocks the reader deterministically.
        cond = ScriptedCondition()
        io = SingleBlockingIO(cond)

        def produce():
            io.write("data")
            io.flush()

        cond.on_wait = produce

        line = io.readline()

        assert line == "data\n"
        # It genuinely parked once and signalled the waiting producer.
        assert cond.wait_calls == 1
        assert cond.notify_calls >= 1
        # The value is cleared after being handed out.
        assert io.value is None
