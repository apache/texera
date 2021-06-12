from abc import ABC
from typing import List, Optional, Tuple

from worker.models.generated.virtualidentity_pb2 import LinkIdentity, ActorVirtualIdentity
from worker.models.payload import DataPayload, DataFrame
from worker.models.tuple import ITuple


class DataSendingPolicy(ABC):

    def __init__(self, policy_tag: LinkIdentity, batch_size: int, receivers: List[ActorVirtualIdentity]):
        self.policy_tag = policy_tag
        self.batch_size = batch_size
        self.receivers = receivers

    def add_tuple_to_batch(self, tuple_: ITuple) -> Optional[Tuple[ActorVirtualIdentity, DataPayload]]:
        pass

    def no_more(self) -> Optional[Tuple[ActorVirtualIdentity, DataPayload]]:
        pass

    def reset(self) -> None:
        pass


class OneToOnePolicy(DataSendingPolicy):
    def __init__(self, policy_tag: LinkIdentity, batch_size: int, receivers: List[ActorVirtualIdentity]):
        super().__init__(policy_tag, batch_size, receivers)

        self.batch = list()

    def add_tuple_to_batch(self, tuple_: ITuple) -> Optional[Tuple[ActorVirtualIdentity, DataPayload]]:
        self.batch.append(tuple_)
        if len(self.batch) == self.batch_size:
            ret_batch = self.batch
            self.batch = list()
            return self.receivers[0], DataFrame(ret_batch)
        else:
            return None
