package engine.architecture.linksemantics

import engine.architecture.deploysemantics.layer.ActorLayer
import engine.architecture.sendsemantics.datatransferpolicy.OneToOnePolicy
import engine.architecture.sendsemantics.routees.DirectRoutee
import engine.common.ambermessage.PrincipalMessage.{GetInputLayer, GetOutputLayer}
import engine.common.ambermessage.WorkerMessage.UpdateOutputLinking
import engine.common.ambertag.LinkTag
import engine.common.{AdvancedMessageSending, Constants, TupleSinkOperatorExecutor}
import engine.operators.OpExecConfig
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

//ugly design, but I don't know how to make it better
class OperatorLink(val from: (OpExecConfig, ActorRef), val to: (OpExecConfig, ActorRef))
    extends Serializable {
  implicit val timeout: Timeout = 5.seconds
  var linkStrategy: LinkStrategy = _
  lazy val tag: LinkTag = linkStrategy.tag
  def link()(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    val sender = Await.result(from._2 ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
    val receiver = Await.result(to._2 ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
    if (linkStrategy == null) {
      //TODO: use type matching to generate a 'smarter' strategy based on the operators
      if (to._1.requiredShuffle) {
        linkStrategy = new HashBasedShuffle(
          sender,
          receiver,
          Constants.defaultBatchSize,
          to._1.getShuffleHashFunction(sender.tag)
        )
      } else if (
        to._1.isInstanceOf[TupleSinkOperatorExecutor]
      ) {
        linkStrategy = new AllToOne(sender, receiver, Constants.defaultBatchSize)
      } else if (sender.layer.length == receiver.layer.length) {
        linkStrategy = new LocalOneToOne(sender, receiver, Constants.defaultBatchSize)
      } else {
        linkStrategy = new LocalRoundRobin(sender, receiver, Constants.defaultBatchSize)
      }
    } else {
      linkStrategy.from.layer = sender.layer
      linkStrategy.to.layer = receiver.layer
    }
    linkStrategy.link()
  }

}
