from pytexera import *


class CountBatchOperator(UDFBatchOperator):
    BATCH_SIZE = 10

    def __init__(self, *args):
        if len(args) >= 1:
            self.BATCH_SIZE = args[0]
        super().__init__()
        self.count = 0

    @overrides
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        self.count += 1
        yield batch
