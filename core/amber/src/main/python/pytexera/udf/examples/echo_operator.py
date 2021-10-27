from typing import Iterator, Optional, Union

from pytexera import InputExhausted, Tuple, TupleLike, UDFOperator, overrides


class EchoOperator(UDFOperator):

    @overrides
    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: int) -> Iterator[Optional[TupleLike]]:
        if isinstance(tuple_, Tuple):
            yield tuple_
