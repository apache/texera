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

import pytest

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

    def test_keeps_waiting_across_spurious_wakeups(self):
        # A wakeup that does not publish a value must not end the loop:
        # readline() has to re-check `value` and park again, otherwise pdb
        # would receive None as a command line.
        cond = ScriptedCondition()
        io = SingleBlockingIO(cond)

        def produce_on_third_wakeup():
            if cond.wait_calls == 3:
                io.write("late")
                io.flush()

        cond.on_wait = produce_on_third_wakeup

        assert io.readline() == "late\n"
        assert cond.wait_calls == 3
        # Every parking round also notifies the producer side.
        assert cond.notify_calls == 3

    def test_ignores_a_limit_argument(self):
        # pdb calls readline() through the IO API, which may pass a limit;
        # the value is always handed out whole regardless.
        io = SingleBlockingIO(Condition())
        io.write("abcdef")
        io.flush()

        assert io.readline(2) == "abcdef\n"

    def test_clears_the_value_even_when_waiting_raises(self):
        # The clear lives in a `finally`, so an interrupted wait must not
        # leave a stale line behind for the next reader to pick up.
        cond = ScriptedCondition()
        io = SingleBlockingIO(cond)

        def explode():
            io.value = "half-delivered\n"
            raise KeyboardInterrupt

        cond.on_wait = explode

        with pytest.raises(KeyboardInterrupt):
            io.readline()

        assert io.value is None

    def test_consecutive_lines_are_delivered_in_order(self):
        # The IO holds one line at a time; a write/flush/readline cycle must
        # be repeatable without any state leaking between lines.
        io = SingleBlockingIO(Condition())

        for line in ("first", "second", "third"):
            io.write(line)
            io.flush()
            assert io.readline() == f"{line}\n"
            assert io.value is None
            assert io.buf == ""

    def test_a_flush_overwrites_an_unread_value(self):
        # Documented single-element semantics: there is no queue, so a second
        # flush before a read replaces the pending line.
        io = SingleBlockingIO(Condition())
        io.write("stale")
        io.flush()
        io.write("fresh")
        io.flush()

        assert io.readline() == "fresh\n"


class TestUnsupportedIOOperations:
    """The remaining IO methods are deliberate no-ops: pdb never calls them,
    but SingleBlockingIO is handed to pdb as a full IO, so they must be inert
    rather than raising or returning junk."""

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda io: io.close(), id="close"),
            pytest.param(lambda io: io.fileno(), id="fileno"),
            pytest.param(lambda io: io.isatty(), id="isatty"),
            pytest.param(lambda io: io.read(4), id="read"),
            pytest.param(lambda io: io.readable(), id="readable"),
            pytest.param(lambda io: io.readlines(4), id="readlines"),
            pytest.param(lambda io: io.seek(0, 0), id="seek"),
            pytest.param(lambda io: io.seekable(), id="seekable"),
            pytest.param(lambda io: io.tell(), id="tell"),
            pytest.param(lambda io: io.truncate(0), id="truncate"),
            pytest.param(lambda io: io.writable(), id="writable"),
            pytest.param(lambda io: io.writelines(["a", "b"]), id="writelines"),
            pytest.param(lambda io: io.__next__(), id="next"),
            pytest.param(lambda io: io.__iter__(), id="iter"),
            pytest.param(lambda io: io.__enter__(), id="enter"),
            pytest.param(lambda io: io.__exit__(None, None, None), id="exit"),
        ],
    )
    def test_no_op_methods_return_none_without_raising(self, call):
        io = SingleBlockingIO(Condition())

        assert call(io) is None

    def test_no_op_methods_do_not_disturb_the_pending_line(self):
        io = SingleBlockingIO(Condition())
        io.write("payload")
        io.flush()

        io.close()
        io.writelines(["ignored"])
        io.truncate(0)

        # None of the inert methods may consume or mutate the buffered line.
        assert io.readline() == "payload\n"
