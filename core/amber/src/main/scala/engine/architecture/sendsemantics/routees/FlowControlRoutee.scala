package engine.architecture.sendsemantics.routees

import engine.common.AdvancedMessageSending
import engine.common.ambermessage.WorkerMessage.UpdateInputLinking
import engine.common.ambertag.LinkTag
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class FlowControlRoutee(receiver: ActorRef) extends ActorRoutee(receiver) {
  var context: ActorContext = _

  override def initialize(tag: LinkTag)(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    senderActor = ac.actorOf(FlowControlSenderActor.props(receiver))
    context = ac
    AdvancedMessageSending.blockingAskWithRetry(
      receiver,
      UpdateInputLinking(senderActor, tag.from),
      10
    )
  }

  override def toString: String = s"FlowControlRoutee($receiver)"

  override def reset(): Unit = {
    super.reset()
    senderActor = context.actorOf(FlowControlSenderActor.props(receiver))
  }
}
