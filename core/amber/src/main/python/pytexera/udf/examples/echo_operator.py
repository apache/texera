from typing import Iterator, Optional

from pytexera import Tuple, TupleLike, UDFOperator, overrides


class EchoOperator(UDFOperator):

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
