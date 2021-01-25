package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  OneToOnePolicy,
  RoundRobinPolicy
}

import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  UpdateInputLinking,
  AddDataSendingPolicy
}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class AllToOne(from: WorkerLayer, to: WorkerLayer, batchSize: Int)
    extends LinkStrategy(from, to, batchSize) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext
  ): Unit = {
    assert(from.isBuilt && to.isBuilt && to.layer.size == 1)
    val toActor = to.identifiers.head
    from.layer.foreach(x => {
//      val routee = if(x.path.address.hostPort == toActor.path.address.hostPort) new DirectRoutee(toActor) else new FlowControlRoutee(toActor)
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        AddDataSendingPolicy(
          new OneToOnePolicy(tag, batchSize, Array(toActor))
        ),
        10
      )
    })
  }
}
