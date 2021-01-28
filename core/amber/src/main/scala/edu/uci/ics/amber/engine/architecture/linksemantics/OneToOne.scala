package edu.uci.ics.amber.engine.architecture.linksemantics

import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{DataSendingPolicy, OneToOnePolicy}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.concurrent.ExecutionContext

class OneToOne(from: WorkerLayer, to: WorkerLayer, batchSize: Int, inputNum: Int)
    extends LinkStrategy(from, to, batchSize, inputNum) {
  override def getPolicies():Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] ={
    assert(from.isBuilt && to.isBuilt && from.numWorkers == to.numWorkers)
    from.identifiers.indices.map(i => (from.identifiers(i), new OneToOnePolicy(tag, batchSize, Array(to.identifiers(i))), Seq(to.identifiers(i))))
  }
}
