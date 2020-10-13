package engine.architecture.deploysemantics.layer

import engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import engine.architecture.worker.Generator
import engine.common.ambertag.{LayerTag, WorkerTag}
import engine.common.SourceOperatorExecutor
import engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope

class GeneratorWorkerLayer(
    tag: LayerTag,
    val metadata: Int => SourceOperatorExecutor,
    _numWorkers: Int,
    df: DeploymentFilter,
    ds: DeployStrategy
) extends ActorLayer(tag, _numWorkers, df, ds) {

  var metadataForFirst: SourceOperatorExecutor = _
  var deployForFirst: Address = _

  override def clone(): AnyRef = {
    val res = new GeneratorWorkerLayer(tag, metadata, numWorkers, df, ds)
    res.layer = layer.clone()
    res
  }

  def build(prev: Array[(OpExecConfig, ActorLayer)], all: Array[Address])(implicit
      context: ActorContext
  ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    layer = new Array[ActorRef](numWorkers)
    var idx = 0
    for (i <- 0 until numWorkers) {
      try {
        val workerTag = WorkerTag(tag, i)
        val m = metadata(i)
        val d = deployStrategy.next()
        if (i == 0) {
          metadataForFirst = m
          tagForFirst = workerTag
          deployForFirst = d
        }
        layer(idx) =
          context.actorOf(Generator.props(m, workerTag).withDeploy(Deploy(scope = RemoteScope(d))))
        idx += 1
      } catch {
        case e: Exception => println(e)
      }
    }
    if (idx != numWorkers) {
      layer = layer.take(idx)
      numWorkers = idx + 1
    }
  }

  override def getFirstMetadata: Any = metadataForFirst
}
