from typing import Iterator, Optional, Union

from overrides import overrides

from core import TupleLike, InputExhausted, Tuple, UDFOperator
from proto.edu.uci.ics.amber.engine.common import LinkIdentity


class EchoOperator(UDFOperator):

    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) \
            -> Iterator[Optional[TupleLike]]:
        if isinstance(tuple_, Tuple):
            yield tuple_
