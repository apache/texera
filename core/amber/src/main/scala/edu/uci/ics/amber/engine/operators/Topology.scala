//package edu.uci.ics.amber.engine.operators
//
//import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
//import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
//import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
//import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
//import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.makeLayer
//import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
//import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
//
//
//object Topology {
//  def singleLayer(workerLayer: WorkerLayer): Topology = {
//    new Topology(Array(workerLayer), Array())
//  }
//
//  def to
//}
//
//case class Topology(
//                     layers: Array[WorkerLayer], links: Array[LinkIdentity]
//              ) extends Serializable {
//
////  // topology only supports a chain of layers,
////  // the link order must follow the layer order
////  assert(layers.length == links.length + 1)
////  links.indices.foreach(i => {
////    assert(layers(i).id == links(i).from)
////    assert(layers(i + 1).id == links(i).to)
////  })
//}
//
