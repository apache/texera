package Engine.Architecture.SendSemantics.Routees


import Engine.Common.AmberMessage.ControlMessage.{AckOfEndSending, AckWithSequenceNumber, Pause, RequireAck, Resume}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props, Stash}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object FlowControlSenderActor{
  def props(receiver:ActorRef): Props = Props(new FlowControlSenderActor(receiver))

  final val maxWindowSize = 64
  final val minWindowSize = 2
  final val expendThreshold = 4
  final val sendingTimeout:FiniteDuration = 30.seconds
//  final val activateBackPressureThreshold = 512
//  final val deactivateBackPressureThreshold = 32
  final val logicalTimeGap = 16
  final case class EndSendingTimedOut()
  final case class MessageTimedOut(seq:Long)
}


class FlowControlSenderActor(val receiver:ActorRef) extends Actor with Stash{
  import FlowControlSenderActor._

  implicit val timeout:Timeout = 1.second
  implicit val ec:ExecutionContext = context.dispatcher

  var exactlyCount = 0
  var ssThreshold = 16
  var windowSize = 2
  var maxSentSequenceNumber = 0L
  var handleOfEndSending:(Long,Cancellable) = _
//  var backPressureActivated = false

  val messagesOnTheWay = new mutable.LongMap[(Cancellable,DataMessage)]
  val messagesToBeSent = new mutable.Queue[DataMessage]

  override def receive: Receive = {
    case msg:DataMessage =>
      if(messagesOnTheWay.size < windowSize){
        maxSentSequenceNumber = Math.max(maxSentSequenceNumber,msg.sequenceNumber)
        messagesOnTheWay(msg.sequenceNumber) = (context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(msg.sequenceNumber)),msg)
        receiver ! RequireAck(msg)
      }else{
        messagesToBeSent.enqueue(msg)
//        if(messagesToBeSent.size >= activateBackPressureThreshold){
//          //producer produces too much data, the network cannot handle it, activate back pressure
//          backPressureActivated = true
//          context.parent ! ActivateBackPressure
//        }
      }
    case msg:EndSending =>
      //always send end-sending message regardless the message queue size
      handleOfEndSending = (msg.sequenceNumber,context.system.scheduler.scheduleOnce(sendingTimeout,self,EndSendingTimedOut))
      receiver ! RequireAck(msg)
    case AckWithSequenceNumber(seq) =>
      if(messagesOnTheWay.contains(seq)) {
        messagesOnTheWay(seq)._1.cancel()
        messagesOnTheWay.remove(seq)
        if (maxSentSequenceNumber-seq < logicalTimeGap) {
          if (windowSize < ssThreshold) {
            windowSize = Math.min(windowSize * 2, ssThreshold)
          } else {
            windowSize += 1
          }
        } else {
          ssThreshold /= 2
          windowSize = Math.max(minWindowSize,Math.min(ssThreshold,maxWindowSize))
        }
        if(messagesOnTheWay.size < windowSize && messagesToBeSent.nonEmpty){
          val msg = messagesToBeSent.dequeue()
          maxSentSequenceNumber = Math.max(maxSentSequenceNumber,msg.sequenceNumber)
          messagesOnTheWay(msg.sequenceNumber) = (context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(msg.sequenceNumber)),msg)
          receiver ! RequireAck(msg)
        }
      }
    case AckOfEndSending =>
      if(handleOfEndSending != null){
        handleOfEndSending._2.cancel()
        handleOfEndSending = null
      }
    case EndSendingTimedOut =>
      if(handleOfEndSending != null){
        handleOfEndSending = (handleOfEndSending._1,context.system.scheduler.scheduleOnce(sendingTimeout,self,EndSendingTimedOut))
        receiver ! RequireAck(EndSending(handleOfEndSending._1))
      }
    case MessageTimedOut(seq) =>
      if(messagesOnTheWay.contains(seq)){
        //resend the data message
        val msg = messagesOnTheWay(seq)._2
        messagesOnTheWay(msg.sequenceNumber) = (context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(msg.sequenceNumber)),msg)
        receiver ! RequireAck(msg)
      }
    case Resume =>
    case Pause => context.become(paused)
  }

  final def paused:Receive ={
    case Pause =>
    case Resume =>
      context.become(receive)
      unstashAll()
    case msg => stash()
  }
}
