package edu.uci.ics.amber.engine.architecture.messaging

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.DataMessage
import edu.uci.ics.amber.engine.common.tuple.ITuple

class MessagingManager(val fifoEnforcer: FIFOAccessPort) {

  var nextBatches: Option[Array[Array[ITuple]]] = None
  var nextBatchIterator: Iterator[Array[ITuple]] = Iterator.empty

  def receiveMessage(msg: DataMessage, sender: ActorRef): Unit = {
    nextBatches = fifoEnforcer.preCheck(msg.sequenceNumber, msg.payload, sender)
    nextBatchIterator = nextBatches match {
      case Some(batches) =>
        batches.iterator
      case None =>
        Iterator.empty
    }
  }

  def hasNextBatch(): Boolean = {
    nextBatchIterator.hasNext
  }

  def getNextBatch(): Array[ITuple] = {
    nextBatchIterator.next()
  }
}
