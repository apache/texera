package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable.ArrayBuffer

trait Partitioner extends Serializable {
  def getBucketIndex(tuple: Tuple): Iterator[Int]

  def allReceivers: Seq[ActorVirtualIdentity]
}

class NetworkOutputBuffer(
    val to: ActorVirtualIdentity,
    val dataOutputPort: NetworkOutputGateway,
    val batchSize: Int = AmberConfig.defaultBatchSize
) {

  var buffer = new ArrayBuffer[ITuple]()

  def addTuple(tuple: ITuple): Unit = {
    buffer.append(tuple)
    if (buffer.size >= batchSize) {
      flush()
    }
  }

  def noMore(): Unit = {
    flush()
    dataOutputPort.sendTo(to, EndOfUpstream())
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      dataOutputPort.sendTo(to, DataFrame(buffer.toArray))
      buffer = new ArrayBuffer[ITuple]()
    }
  }

}
