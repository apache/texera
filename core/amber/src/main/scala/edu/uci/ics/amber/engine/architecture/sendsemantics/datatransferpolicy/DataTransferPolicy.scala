package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

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
  def addToBatch(tuple: ITuple)(implicit sender: ActorRef = Actor.noSender): Option[(ActorRef,Array[ITuple])]

  def noMore()(implicit sender: ActorRef = Actor.noSender): Array[(ActorRef,Array[ITuple])]

  def initialize(linkTag: LinkTag, receivers: Array[ActorRef])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    this.tag = linkTag
    receivers.foreach(x => log.info("link: {}", x))
  }

  def reset(): Unit

}
