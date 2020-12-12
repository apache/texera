package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.DataMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

class MessagingManager(val fifoEnforcer: FIFOAccessPort) {

  // TODO: There are so many ways to represent input operator in a worker - int, LayerTag, ActorRef. We should choose one.
  var nextDataPayloads: Array[(LayerTag, Array[ITuple])] = _
  var nextDataPayloadIterator: Iterator[(LayerTag, Array[ITuple])] = Iterator.empty

  /**
    * Receives the WorkflowMessage, strips the sequence number away and produces a payload.
    * The payload can be accessed by the actors by calling corresponding hasNext and getNext.
    * @param message
    * @param sender
    */
  def receiveMessage(message: WorkflowMessage, sender: ActorRef): Unit = {
    message match {
      case dataMsg: DataMessage =>
        val nextDataBatches: Option[Array[Array[ITuple]]] =
          fifoEnforcer.preCheck(dataMsg.sequenceNumber, dataMsg.payload, sender)
        nextDataBatches match {
          case Some(batches) =>
            val currentEdge = fifoEnforcer.actorToEdge(sender)
            nextDataPayloads = batches.map(b => (currentEdge, b))
            nextDataPayloadIterator = nextDataPayloads.iterator
          case None =>
            nextDataPayloadIterator = Iterator.empty
        }

      case controlMsg =>
        // To be implemented later
        throw new NotImplementedError(
          "receive message for messaging manager shouldn't be called for control message"
        )
    }
  }

  def hasNextDataPayload(): Boolean = {
    nextDataPayloadIterator.hasNext
  }

  def getNextDataPayload(): (LayerTag, Array[ITuple]) = {
    nextDataPayloadIterator.next()
  }
}
