package engine.architecture.linksemantics

import engine.architecture.deploysemantics.layer.ActorLayer
import engine.architecture.sendsemantics.datatransferpolicy.{
  HashBasedShufflePolicy,
  RoundRobinPolicy
}
import engine.architecture.sendsemantics.routees.{DirectRoutee, FlowControlRoutee}
import engine.common.AdvancedMessageSending
import engine.common.ambermessage.WorkerMessage.{UpdateInputLinking, UpdateOutputLinking}
import engine.common.tuple.Tuple
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class HashBasedShuffle(from: ActorLayer, to: ActorLayer, batchSize: Int, hashFunc: Tuple => Int)
    extends LinkStrategy(from, to, batchSize) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt)
    from.layer.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        UpdateOutputLinking(
          new HashBasedShufflePolicy(batchSize, hashFunc),
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
