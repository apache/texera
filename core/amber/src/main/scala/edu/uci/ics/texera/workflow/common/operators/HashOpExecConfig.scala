//package edu.uci.ics.texera.workflow.common.operators
//import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
//import edu.uci.ics.amber.engine.common.Constants
//import edu.uci.ics.amber.engine.common.Constants.defaultBatchSize
//import edu.uci.ics.amber.engine.common.virtualidentity.{
//  ActorVirtualIdentity,
//  LayerIdentity,
//  OperatorIdentity
//}
//
//class HashOpExecConfig(
//    override val id: OperatorIdentity,
//    override val opExec: Int => OperatorExecutor,
//    hashColumnIndices: Array[Int]
//) extends OneToOneTopology(id, opExec) {
//
//  partitionRequirement = List(
//    Option(HashBasedShufflePartitioning(defaultBatchSize, List(), hashColumnIndices))
//  )
//
//  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = hashColumnIndices
//
//}
