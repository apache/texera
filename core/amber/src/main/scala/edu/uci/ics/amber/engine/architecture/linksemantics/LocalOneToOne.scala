package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  OneToOnePolicy,
  RoundRobinPolicy
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.DirectRoutee
import edu.uci.ics.amber.engine.common.AdvancedMessageSending
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  UpdateInputLinking,
  UpdateOutputLinking
}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class LocalOneToOne(from: ActorLayer, to: ActorLayer, batchSize: Int, inputNum: Int)
    extends LinkStrategy(from, to, batchSize, inputNum) {
  override def link()(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    assert(from.isBuilt && to.isBuilt && from.layer.length == to.layer.length)
    val froms: Map[String, Array[ActorRef]] =
      from.layer.groupBy(actor => actor.path.address.hostPort)
    val tos: Map[String, Array[ActorRef]] = to.layer.groupBy(actor => actor.path.address.hostPort)
    if (!(froms.keySet == tos.keySet && froms.forall(x => x._2.length == tos(x._1).length))) {
      println("err")
    }
    assert(froms.keySet == tos.keySet && froms.forall(x => x._2.length == tos(x._1).length))
    froms.foreach(x => {
      for (i <- x._2.indices) {
        AdvancedMessageSending.blockingAskWithRetry(
          x._2(i),
          UpdateOutputLinking(
            new OneToOnePolicy(batchSize),
            tag,
            Array(new DirectRoutee(tos(x._1)(i)))
          ),
          10
        )
      }
    })
  }
}
