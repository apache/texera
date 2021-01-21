package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.RoundRobinPolicy
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  UpdateInputLinking,
  AddDataSendingPolicy
}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class FullRoundRobin(from: WorkerLayer, to: WorkerLayer, batchSize: Int, inputNum: Int)
    extends LinkStrategy(from, to, batchSize, inputNum) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext
  ): Unit = {
    assert(from.isBuilt && to.isBuilt)
    //TODO:change routee type according to the machine address
    from.layer.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        AddDataSendingPolicy(
          new RoundRobinPolicy(tag, batchSize, to.identifiers)
        ),
        10
      )
    )
  }
}
