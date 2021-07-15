package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

abstract class Partitioner() {
  val partitioning: Partitioning

  /**
    * Keeps on adding tuples to the batch. When the batch_size is reached, the batch is returned along with the receiver
    * to send the batch to.
    * @param tuple ITuple to be added.
    * @return When return condition is met, return the (to: ActorVirtualIdentity, payload:
    */
  def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)]

  def noMore(): Array[(ActorVirtualIdentity, DataPayload)]

  def reset(): Unit

}
