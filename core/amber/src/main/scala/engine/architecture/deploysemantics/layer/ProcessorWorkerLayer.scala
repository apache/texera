package engine.architecture.deploysemantics.layer

import engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import engine.architecture.worker.{Generator, Processor}
import engine.common.ambertag.{LayerTag, WorkerTag}
import engine.common.OperatorExecutor
import engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope

class ProcessorWorkerLayer(
    tag: LayerTag,
    val metadata: Int => OperatorExecutor,
    _numWorkers: Int,
    df: DeploymentFilter,
    ds: DeployStrategy
) extends ActorLayer(tag, _numWorkers, df, ds) {

  var metadataForFirst: OperatorExecutor = _
  var deployForFirst: Address = _

  override def clone(): AnyRef = {
    val res = new ProcessorWorkerLayer(tag, metadata, numWorkers, df, ds)
    res.layer = layer.clone()
    res
  }

  def build(prev: Array[(OpExecConfig, ActorLayer)], all: Array[Address])(implicit
      context: ActorContext
  ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    layer = new Array[ActorRef](numWorkers)
    for (i <- 0 until numWorkers) {
      val workerTag = WorkerTag(tag, i)
      val m = metadata(i)
      val d = deployStrategy.next()
      if (i == 0) {
        metadataForFirst = m
        tagForFirst = workerTag
        deployForFirst = d
      }
      layer(i) =
        context.actorOf(Processor.props(m, workerTag).withDeploy(Deploy(scope = RemoteScope(d))))
    }
  }

  override def getFirstMetadata: Any = metadataForFirst
}
