from collections import OrderedDict
from typing import Iterable, Tuple

from worker.architecture.sendsemantics.data_sending_policy import DataSendingPolicy
from worker.models.generated.virtualidentity_pb2 import ActorVirtualIdentity, LinkIdentity
from worker.models.payload import DataPayload
from worker.models.tuple import ITuple


class TupleToBatchConverter:

    def __init__(self, ):
        self._policies: OrderedDict[LinkIdentity, DataSendingPolicy] = OrderedDict()

    def add_policy(self, policy: DataSendingPolicy) -> None:
        """
        Add down stream operator and its transfer policy
        :param policy:
        :return:
        """
        self._policies.update({policy.policy_tag: policy})

    def tuple_to_batch(self, tuple_: ITuple) -> Iterable[Tuple[ActorVirtualIdentity, DataPayload]]:
        return filter(lambda x: x, map(lambda policy: policy.add_tuple_to_batch(tuple_), self._policies.values()))

    def emit_end_of_upstream(self) -> None:
        # TODO: finish this
        pass
