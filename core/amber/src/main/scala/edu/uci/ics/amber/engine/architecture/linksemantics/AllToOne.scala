package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{OneToOnePolicy, RoundRobinPolicy}
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.{DirectRoutee, FlowControlRoutee}
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{UpdateInputLinking, UpdateOutputLinking}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class AllToOne(from: ActorLayer, to: ActorLayer, batchSize: Int)
    extends LinkStrategy(from, to, batchSize) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt && to.layer.length == 1)
    val toActor = to.layer(0)
    from.layer.foreach(x => {
//      val routee = if(x.path.address.hostPort == toActor.path.address.hostPort) new DirectRoutee(toActor) else new FlowControlRoutee(toActor)
      // TODO: hack for demo fault tolerance
      val routee =
        if (x.path.address.hostPort == toActor.path.address.hostPort) new DirectRoutee(toActor)
        else new DirectRoutee(toActor)
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        UpdateOutputLinking(new OneToOnePolicy(batchSize), tag, Array(routee)),
        10
      )
    })
  }
}
