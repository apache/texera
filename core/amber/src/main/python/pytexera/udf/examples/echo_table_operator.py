from pytexera import *


class EchoTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, input_: int) -> Iterator[Optional[TableLike]]:
        yield table
