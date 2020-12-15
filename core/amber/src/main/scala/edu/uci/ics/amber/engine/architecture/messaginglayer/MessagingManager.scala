package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, LinkTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class MessagingManager(val fifoEnforcer: FIFOAccessPort) {

  // Message Receiving part
  // TODO: There are so many ways to represent input operator in a worker - int, LayerTag, ActorRef. We should choose one.
  private var nextInputDataPayloads: Array[(LayerTag, Array[ITuple])] = _
  private var nextInputDataPayloadIterator: Iterator[(LayerTag, Array[ITuple])] = Iterator.empty

  // Message Sending Part
  private var receiverToDataSender: mutable.Map[ActorRef, BaseRoutee] =
    mutable.Map[ActorRef, BaseRoutee]()
  private var receiverToDataSequenceNumbers: mutable.Map[ActorRef, Long] =
    mutable.Map[ActorRef, Long]()

  // TODO: Find a way to remove the usage of implicits
  def updateReceiverAndSender(_dataSenders: Array[BaseRoutee], tag: LinkTag)(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    _dataSenders.foreach(_dataSender => {
      _dataSender.initialize(tag)
      receiverToDataSender += (_dataSender.receiver -> _dataSender)
      receiverToDataSequenceNumbers += (_dataSender.receiver -> 0)
    })
  }

  def sendDataMessage(receiver: ActorRef, dataBatch: Array[ITuple])(implicit
      sender: ActorRef
  ): Unit = {
    receiverToDataSender(receiver).schedule(
      DataMessage(receiverToDataSequenceNumbers(receiver), dataBatch)
    )(sender)
    receiverToDataSequenceNumbers(receiver) += 1
  }

  def sendEndDataMessage()(implicit
      sender: ActorRef
  ): Unit = {
    receiverToDataSender.keys.foreach(receiver => {
      receiverToDataSender(receiver).schedule(EndSending(receiverToDataSequenceNumbers(receiver)))
      receiverToDataSequenceNumbers(receiver) += 1
    })
  }

  def disposeDataSenders(): Unit = {
    receiverToDataSender.values.foreach(_.dispose())
  }

  def resetDataSending(): Unit = {
    receiverToDataSender.values.foreach(_.reset())
    receiverToDataSender.keys.foreach(receiver => receiverToDataSequenceNumbers(receiver)=0)
  }

  def pauseDataSending(): Unit = {
    receiverToDataSender.values.foreach(_.pause())
  }

  def resumeDataSending()(implicit
      sender: ActorRef
  ): Unit = {
    receiverToDataSender.values.foreach(_.resume())
  }

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
            nextInputDataPayloads = batches.map(b => (currentEdge, b))
            nextInputDataPayloadIterator = nextInputDataPayloads.iterator
          case None =>
            nextInputDataPayloadIterator = Iterator.empty
        }

      case controlMsg =>
        // To be implemented later
        throw new NotImplementedError(
          "receive message for messaging manager shouldn't be called for control message"
        )
    }
  }

  def hasNextDataPayload(): Boolean = {
    nextInputDataPayloadIterator.hasNext
  }

  def getNextDataPayload(): (LayerTag, Array[ITuple]) = {
    nextInputDataPayloadIterator.next()
  }
}
