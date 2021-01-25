package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.ambermessage.PrincipalMessage.{GetInputLayer, GetOutputLayer}
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.{
  AdvancedMessageSending,
  Constants,
  ITupleSinkOperatorExecutor
}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, SinkOpExecConfig}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

//ugly design, but I don't know how to make it better
class OperatorLink(val from: (OpExecConfig, WorkerLayer), val to: (OpExecConfig, WorkerLayer))
    extends Serializable {
  implicit val timeout: Timeout = 5.seconds
  var linkStrategy: LinkStrategy = _
  lazy val tag: LinkTag = linkStrategy.tag
  def link()(implicit timeout: Timeout, ec: ExecutionContext): Unit = {
    val sender = from._2
    val receiver = to._2
    if (linkStrategy == null) {
      //TODO: use type matching to generate a 'smarter' strategy based on the operators
      if (to._1.requiredShuffle) {
        linkStrategy = new HashBasedShuffle(
          sender,
          receiver,
          Constants.defaultBatchSize,
          to._1.getShuffleHashFunction(sender.tag)
        )
      } else if (to._1.isInstanceOf[SinkOpExecConfig]) {
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
