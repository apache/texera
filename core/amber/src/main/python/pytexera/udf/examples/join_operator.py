from collections import defaultdict

from pytexera import *


# needs the dual-input-ports PythonUDF
class JoinOperator(UDFOperatorV2):
    @overrides
    def open(self) -> None:
        self.left_dict = defaultdict(list)

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        if port == 0:
            # building the hashmap
            self.left_dict[tuple_['key']].append(tuple_)
        else:
            # probing the hashmap
            for left_tuple in self.left_dict.get(tuple_['key'], []):
                # join and output
                yield left_tuple + tuple_
