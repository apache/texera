from typing import Iterator, Optional

from pytexera import Table, TableLike, UDFTableOperator, overrides


class EchoTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, input_: int) -> Iterator[Optional[TableLike]]:
        yield table
