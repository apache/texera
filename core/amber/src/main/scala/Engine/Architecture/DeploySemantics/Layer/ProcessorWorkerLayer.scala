package Engine.Architecture.DeploySemantics.Layer

import Engine.Architecture.DeploySemantics.DeployStrategy.DeployStrategy
import Engine.Architecture.DeploySemantics.DeploymentFilter.DeploymentFilter
import Engine.Architecture.Worker.{Generator, Processor}
import Engine.Common.AmberTag.{LayerTag, WorkerTag}
import Engine.Common.TupleProcessor
import Engine.Operators.OperatorMetadata
import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope

class ProcessorWorkerLayer(tag:LayerTag, val metadata: Int => TupleProcessor, _numWorkers:Int, df: DeploymentFilter, ds: DeployStrategy) extends ActorLayer(tag,_numWorkers,df,ds) {

  var metadataForFirst:TupleProcessor = _
  var deployForFirst:Address = _

  override def clone(): AnyRef = {
    val res = new ProcessorWorkerLayer(tag,metadata,numWorkers,df,ds)
    res.layer = layer.clone()
    res
  }


  def build(prev:Array[(OperatorMetadata,ActorLayer)],all:Array[Address])(implicit context:ActorContext): Unit ={
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    layer = new Array[ActorRef](numWorkers)
    for(i <- 0 until numWorkers){
      val workerTag = WorkerTag(tag,i)
      val m = metadata(i)
      val d = deployStrategy.next()
      if(i == 0){
        metadataForFirst = m
        tagForFirst = workerTag
        deployForFirst = d
      }
      layer(i)=context.actorOf(Processor.props(m,workerTag).withDeploy(Deploy(scope = RemoteScope(d))))
    }
  }

  override def getFirstMetadata:Any = metadataForFirst
}
