package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.ambermessage.neo.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.concurrent.ExecutionContext

abstract class DataTransferPolicy(var batchSize: Int) extends Serializable {
  var tag: LinkTag = _

  /**
    * Keeps on adding tuples to the batch. When the batch_size is reached, the batch is returned along with the receiver
    * to send the batch to.
    * @param tuple
    * @param sender
    * @return
    */
  def addTupleToBatch(tuple: ITuple): Option[(ActorVirtualIdentity, DataPayload)]

  def noMore(): Array[(ActorVirtualIdentity, DataPayload)]

  def initialize(linkTag: LinkTag, receivers: Array[ActorVirtualIdentity]): Unit = {
    this.tag = linkTag
  }

  def reset(): Unit

}
