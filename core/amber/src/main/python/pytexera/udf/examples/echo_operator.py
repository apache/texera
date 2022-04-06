from pytexera import *


class EchoOperator(UDFOperatorV2):

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_

    @overrides
    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        print(f"end of port {port}")
        yield
