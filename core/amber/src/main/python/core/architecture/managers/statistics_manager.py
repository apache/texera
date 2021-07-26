class StatisticsManager:
    def __init__(self):
        self.input_tuple_count = 0
        self.output_tuple_count = 0

    def get_statistics(self):
        return self.input_tuple_count, self.output_tuple_count
