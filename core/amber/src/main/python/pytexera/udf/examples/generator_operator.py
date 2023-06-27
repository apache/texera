from pytexera import *


class GeneratorOperator(UDFSourceOperator):
    @overrides
    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        yield {"test": [1, 2, 3]}
