package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  HashBasedShufflePolicy,
  RoundRobinPolicy
}
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  UpdateInputLinking,
  AddDataSendingPolicy
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class HashBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    hashFunc: ITuple => Int
) extends LinkStrategy(from, to, batchSize) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext
  ): Unit = {
    assert(from.isBuilt && to.isBuilt)
    from.layer.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        AddDataSendingPolicy(
          new HashBasedShufflePolicy(tag, batchSize, hashFunc, to.identifiers)
        ),
        10
      )
    )
  }
}
