package Engine.Architecture.LinkSemantics

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.SendSemantics.DataTransferPolicy.{HashBasedShufflePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.{DirectRoutee, FlowControlRoutee}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{UpdateInputLinking, UpdateOutputLinking}
import Engine.Common.AmberTuple.Tuple
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext


class HashBasedShuffle(from:ActorLayer, to:ActorLayer, batchSize:Int, hashFunc:Tuple => Int) extends LinkStrategy(from,to,batchSize) {
  override def link()(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    assert(from.isBuilt && to.isBuilt)
    from.layer.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(x,
        UpdateOutputLinking(new HashBasedShufflePolicy(batchSize,hashFunc),tag, to.layer.map(y => if(x.path.address.hostPort == y.path.address.hostPort) new DirectRoutee(y) else new FlowControlRoutee(y))),
        10))
  }
}
