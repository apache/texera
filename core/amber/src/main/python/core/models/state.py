
class State:
    def __init__(self, key, value):
        self.key = key
        self.value = value


    def __str__(self):
        return f"State[{self.key}: {self.value}"

    __repr__ = __str__
