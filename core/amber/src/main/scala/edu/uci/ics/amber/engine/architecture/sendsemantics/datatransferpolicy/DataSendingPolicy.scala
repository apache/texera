package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

// Sending policy used by a worker to send data to the downstream workers.
abstract class DataSendingPolicy(
    val policyTag: LinkIdentity,
    var batchSize: Int,
    var receivers: Array[ActorVirtualIdentity]
) extends Serializable {
  assert(receivers != null)

  /**
    * Keeps on adding tuples to the batch. When the batch_size is reached, the batch is returned along with the receiver
    * to send the batch to.
    * @param tuple
    * @param sender
    * @return
    */
  def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)]

  def noMore(): Array[(ActorVirtualIdentity, DataPayload)]

  def reset(): Unit

}
