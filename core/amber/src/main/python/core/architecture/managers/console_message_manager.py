from core.util.buffer.timed_buffer import TimedBuffer


class ConsoleMessageManager:
    def __init__(self):
        self.print_buf = TimedBuffer()

    def get_messages(self, force_flush: bool = False):
        return self.print_buf.get(force_flush)
