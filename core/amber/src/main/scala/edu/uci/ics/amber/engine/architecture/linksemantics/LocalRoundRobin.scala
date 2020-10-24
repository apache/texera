package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.RoundRobinPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.{BaseRoutee, DirectRoutee, FlowControlRoutee}
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.UpdateOutputLinking
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class LocalRoundRobin(from: ActorLayer, to: ActorLayer, batchSize: Int)
    extends LinkStrategy(from, to, batchSize) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt)
    val froms = from.layer.groupBy(actor => actor.path.address.hostPort)
    val tos = to.layer.groupBy(actor => actor.path.address.hostPort)
    val isolatedfroms = froms.keySet.diff(tos.keySet)
    val isolatedtos = tos.keySet.diff(froms.keySet)
    val matched = froms.keySet.intersect(tos.keySet)
    matched.foreach(x => {
      val receivers: Array[BaseRoutee] = tos(x).map(new DirectRoutee(_)) ++ isolatedtos
        .flatMap(tos(_))
        .map(new FlowControlRoutee(_))
        .toArray[BaseRoutee]
      froms(x).foreach(y =>
        AdvancedMessageSending.blockingAskWithRetry(
          y,
          UpdateOutputLinking(new RoundRobinPolicy(batchSize), tag, receivers),
          10
        )
      )
    })
    isolatedfroms.foreach(x => {
      froms(x).foreach(y =>
        AdvancedMessageSending.blockingAskWithRetry(
          y,
          UpdateOutputLinking(
            new RoundRobinPolicy(batchSize),
            tag,
            to.layer.map(new FlowControlRoutee(_))
          ),
          10
        )
      )
    })
  }
}
