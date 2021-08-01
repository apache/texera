from typing import Iterator, Optional, Union

from overrides import overrides

from core.models import Tuple
from core.models.tuple import InputExhausted
from core.udf import UDFOperator
from proto.edu.uci.ics.amber.engine.common import LinkIdentity


class EchoOperator(UDFOperator):

    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) \
            -> Iterator[Optional[Tuple]]:
        if isinstance(tuple_, Tuple):
            yield tuple_
