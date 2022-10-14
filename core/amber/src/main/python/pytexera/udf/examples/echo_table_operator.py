from pytexera import *


class EchoTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table
