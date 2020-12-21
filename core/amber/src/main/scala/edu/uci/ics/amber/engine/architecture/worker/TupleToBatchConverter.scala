package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.messaginglayer.MessagingManager
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataTransferPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.common.amberexception.BreakpointException
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks

class TupleToBatchConverter(val sender: ActorRef, val messagingManager: MessagingManager)
    extends BreakpointSupport {
  var output = new Array[DataTransferPolicy](0)
  var skippedInputTuples = new mutable.HashSet[ITuple]
  var skippedOutputTuples = new mutable.HashSet[ITuple]

  def pauseDataTransfer(): Unit = {
    messagingManager.pauseDataSending()
  }

  def resumeDataTransfer(): Unit = {
    messagingManager.resumeDataSending()(sender)
  }

  def endDataTransfer(): Unit = {
    var i = 0
    while (i < output.length) {
      val receiversAndBatches: Array[(ActorRef, Array[ITuple])] = output(i).noMore()(sender)
      receiversAndBatches.foreach(rb => messagingManager.sendDataMessage(rb._1, rb._2)(sender))
      i += 1
    }
    messagingManager.sendEndDataMessage()(sender)
  }

  def cleanUpDataTransfer(): Unit = {
    //clear all data transfer policies
    output = Array()
    messagingManager.disposeDataSenders()
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

  def updateOutput(policy: DataTransferPolicy, tag: LinkTag, senders: Array[BaseRoutee])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    var i = 0
    policy.initialize(tag, senders.map(_.receiver))
    messagingManager.updateReceiverAndSender(senders, tag)(ac, sender, timeout, ec, log)
    Breaks.breakable {
      while (i < output.length) {
        if (output(i).tag == policy.tag) {
          output(i) = policy
          Breaks.break()
        }
        i += 1
      }
      output = output :+ policy
    }
  }

  def resetOutput(): Unit = {
    output.foreach {
      _.reset()
    }
    messagingManager.resetDataSending()
  }

  def passTupleToDownstream(tuple: ITuple): Unit = {
    var i = 0
    while (i < output.length) {
      val receiverAndbatch: Option[(ActorRef, Array[ITuple])] =
        output(i).addTupleToBatch(tuple)(sender)
      receiverAndbatch match {
        case Some(rb) =>
          // send it to messaging layer to be sent downstream
          messagingManager.sendDataMessage(rb._1, rb._2)(sender)
        case None =>
        // Do nothing
      }
      i += 1
    }
  }

}
