package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.DataMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.tuple.ITuple

class MessagingManager(val fifoEnforcer: FIFOAccessPort) {

  var nextDataBatches: Option[Array[Array[ITuple]]] = None
  var nextDataBatchIterator: Iterator[Array[ITuple]] = Iterator.empty

  def receiveMessage(message: WorkflowMessage, sender: ActorRef): Unit = {
    message match {
      case dataMsg: DataMessage =>
        nextDataBatches = fifoEnforcer.preCheck(dataMsg.sequenceNumber, dataMsg.payload, sender)
        nextDataBatchIterator = nextDataBatches match {
          case Some(batches) =>
            batches.iterator
          case None =>
            Iterator.empty
        }

      case controlMsg =>
        // To be implemented later
        throw new NotImplementedError(
          "receive message for messaging manager shouldn't be called for control message"
        )
    }
  }

  def hasNextDataBatch(): Boolean = {
    nextDataBatchIterator.hasNext
  }

  def getNextDataBatch(): Array[ITuple] = {
    nextDataBatchIterator.next()
  }
}
