package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataTransferPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.architecture.worker.BreakpointSupport
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.InternalQueueElement
import edu.uci.ics.amber.engine.common.amberexception.BreakpointException
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, LinkTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks

class MessagingManager(
    val sender: ActorRef,
    val fifoEnforcer: FIFOAccessPort
) extends BreakpointSupport {

  // Message Receiving part - data members
  // TODO: There are so many ways to represent input operator in a worker - int, LayerTag, ActorRef. We should choose one.
  private var nextInputDataPayloads: Array[(LayerTag, Array[ITuple])] = _
  private var nextInputDataPayloadIterator: Iterator[(LayerTag, Array[ITuple])] = Iterator.empty
  // save current batch related information
  private var currentDataPayload: (LayerTag, Array[ITuple]) = _
  private var currentDataFrameIterator: Iterator[ITuple] = Iterator.empty

  // Message Sending Part - data members
  private var receiverToDataSender: mutable.Map[ActorRef, BaseRoutee] =
    mutable.Map[ActorRef, BaseRoutee]()
  private var receiverToDataSequenceNumbers: mutable.Map[ActorRef, Long] =
    mutable.Map[ActorRef, Long]()
  var policies = new Array[DataTransferPolicy](0)
  var skippedInputTuples = new mutable.HashSet[ITuple]
  var skippedOutputTuples = new mutable.HashSet[ITuple]

  // Message Receiving part - functions
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

  def hasNextSenderTuplePair(): Boolean = {
    if (currentDataPayload == null || !currentDataFrameIterator.hasNext) {
      if (hasNextDataPayload()) {
        currentDataPayload = getNextDataPayload()
        currentDataFrameIterator = currentDataPayload._2.iterator
      }
    }

    currentDataFrameIterator.hasNext
  }

  def getNextSenderTuplePair(): (LayerTag, ITuple) = {
    (currentDataPayload._1, currentDataFrameIterator.next())
  }

  private def hasNextDataPayload(): Boolean = {
    nextInputDataPayloadIterator.hasNext
  }

  private def getNextDataPayload(): (LayerTag, Array[ITuple]) = {
    nextInputDataPayloadIterator.next()
  }

  // Message Sending part - functions
  // TODO: Find a way to remove the usage of implicits
  def updateReceiverAndSender(
      policy: DataTransferPolicy,
      tag: LinkTag,
      _dataSenders: Array[BaseRoutee]
  )(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    var i = 0
    policy.initialize(tag, _dataSenders.map(_.receiver))
    _dataSenders.foreach(_dataSender => {
      _dataSender.initialize(tag)
      receiverToDataSender += (_dataSender.receiver -> _dataSender)
      receiverToDataSequenceNumbers += (_dataSender.receiver -> 0)
    })
    Breaks.breakable {
      while (i < policies.length) {
        if (policies(i).tag == policy.tag) {
          policies(i) = policy
          Breaks.break()
        }
        i += 1
      }
      policies = policies :+ policy
    }
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

  def endDataSending(): Unit = {
    var i = 0
    while (i < policies.length) {
      val receiversAndBatches: Array[(ActorRef, Array[ITuple])] = policies(i).noMore()(sender)
      receiversAndBatches.foreach(rb => sendDataMessage(rb._1, rb._2)(sender))
      i += 1
    }
    sendEndDataMessage()(sender)
  }

  def cleanUpDataSending(): Unit = {
    //clear all data transfer policies
    policies = Array()
    receiverToDataSender.values.foreach(_.dispose())
  }

  def transferTuple(tuple: ITuple, tupleId: Long): Unit = {
    if (tuple != null && !skippedOutputTuples.contains(tuple)) {
      var i = 1
      var breakpointTriggered = false
      var needUserFix = false
      while (i < breakpoints.length) {
        breakpoints(i).accept(tuple)
        if (breakpoints(i).isTriggered) {
          breakpoints(i).triggeredTuple = tuple
          breakpoints(i).triggeredTupleId = tupleId
        }
        breakpointTriggered |= breakpoints(i).isTriggered
        needUserFix |= breakpoints(i).needUserFix
        i += 1
      }
      if (!needUserFix) {
        passTupleToDownstream(tuple)
      }
      if (breakpointTriggered) {
        throw new BreakpointException()
      }
    }
  }

  private def passTupleToDownstream(tuple: ITuple): Unit = {
    var i = 0
    while (i < policies.length) {
      val receiverAndbatch: Option[(ActorRef, Array[ITuple])] =
        policies(i).addTupleToBatch(tuple)(sender)
      receiverAndbatch match {
        case Some(rb) =>
          // send it to messaging layer to be sent downstream
          sendDataMessage(rb._1, rb._2)(sender)
        case None =>
        // Do nothing
      }
      i += 1
    }
  }

  def resetDataSending(): Unit = {
    policies.foreach(_.reset())
    receiverToDataSender.values.foreach(_.reset())
    receiverToDataSender.keys.foreach(receiver => receiverToDataSequenceNumbers(receiver) = 0)
  }

  def pauseDataSending(): Unit = {
    receiverToDataSender.values.foreach(_.pause())
  }

  def resumeDataSending()(implicit
      sender: ActorRef
  ): Unit = {
    receiverToDataSender.values.foreach(_.resume())
  }
}
