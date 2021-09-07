from typing import Iterator, Optional, Union

from overrides import overrides

from pyamber import InputExhausted, Tuple, TupleLike, UDFOperator


class EchoOperator(UDFOperator):

    @overrides
    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: int) -> Iterator[Optional[TupleLike]]:
        if isinstance(tuple_, Tuple):
            yield tuple_
