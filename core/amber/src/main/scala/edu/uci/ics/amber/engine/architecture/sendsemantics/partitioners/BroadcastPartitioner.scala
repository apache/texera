package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.BroadcastPartitioning
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

case class BroadcastPartitioner(partitioning: BroadcastPartitioning) extends Partitioner {

  override def getBucketIndex(tuple: ITuple): Iterator[Int] = {
    val it = Iterator[Int]()
    for (i <- partitioning.receivers.indices) {
      it ++ Iterator(i)
    }
    it
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
