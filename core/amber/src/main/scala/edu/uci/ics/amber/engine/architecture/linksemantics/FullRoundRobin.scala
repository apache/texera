package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.RoundRobinPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.{DirectRoutee, FlowControlRoutee}
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{UpdateInputLinking, UpdateOutputLinking}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class FullRoundRobin(from: ActorLayer, to: ActorLayer, batchSize: Int, inputNum: Int)
    extends LinkStrategy(from, to, batchSize, inputNum) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt)
    //TODO:change routee type according to the machine address
    from.layer.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        UpdateOutputLinking(
          new RoundRobinPolicy(batchSize),
          tag,
          to.layer.map(y =>
            if (x.path.address.hostPort == y.path.address.hostPort) new DirectRoutee(y)
            else new FlowControlRoutee(y)
          )
        ),
        10
      )
    )
  }
}
