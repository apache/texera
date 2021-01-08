package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  OneToOnePolicy,
  RoundRobinPolicy
}

import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  UpdateInputLinking,
  UpdateOutputLinking
}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class AllToOne(from: ActorLayer, to: ActorLayer, batchSize: Int, inputNum: Int)
    extends LinkStrategy(from, to, batchSize, inputNum) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt && to.layer.size == 1)
    val toActor = to.identifiers.head
    from.layer.foreach(x => {
//      val routee = if(x.path.address.hostPort == toActor.path.address.hostPort) new DirectRoutee(toActor) else new FlowControlRoutee(toActor)
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        UpdateOutputLinking(new OneToOnePolicy(batchSize), tag, Array(toActor)),
        10
      )
    })
  }
}
