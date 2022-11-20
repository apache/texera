from pytexera import *


class CountBatchOperator(UDFBatchOperator):
    BATCH_COUNT = 0
    BATCH_SIZE = 10

    @overrides
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        self.BATCH_COUNT += 1
        yield batch