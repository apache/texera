import threading


class AtomicInteger:
    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()

    def inc(self, d=1):
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d=1):
        return self.inc(-d)

    def get_and_inc(self, d=1):
        with self._lock:
            old_value = self._value
            self._value += int(d)
            return old_value

    def get_and_dec(self, d=1):
        return self.get_and_inc(-d)

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = int(v)
            return self._value

    def get_and_set(self, v):
        with self._lock:
            old_value = self.value
            self._value = int(v)
            return old_value
