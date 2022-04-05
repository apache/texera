from pytexera import *


class EchoOperator(UDFOperatorV2):

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
