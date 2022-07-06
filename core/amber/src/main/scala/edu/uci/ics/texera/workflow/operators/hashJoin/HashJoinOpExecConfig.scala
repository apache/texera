package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.util.{makeLayer, toOperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class HashJoinOpExecConfig[K](
    id: OperatorIdentity,
    val probeAttributeName: String,
    val buildAttributeName: String,
    val joinType: JoinType,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(id) {

  shuffleType = ShuffleType.HASH_BASED

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          null,
          Constants.currentWorkerNum,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  def getBuildTableLinkId(): LinkIdentity = {
    inputToOrdinalMapping.find(pair => pair._2._1 == 0).get._1
  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val buildLink = inputToOrdinalMapping.find(pair => pair._2._1 == 0).get._1
    val probeLink = inputToOrdinalMapping.find(pair => pair._2._1 == 1).get._1
    val dependerLink = probeLink
    val dependeeLink = buildLink
    workflow.getSources(toOperatorIdentity(dependerLink.from)).foreach { dependerSource =>
      val opId = workflow.getOperator(dependeeLink.from.operator).id
      workflow.getSources(opId).foreach { dependeeSource =>
        if (dependerSource != dependeeSource) {
          workflow.getOperator(dependerSource).topology.layers.head.startAfter(dependeeLink)
        }
      }
    }
    topology.layers.head.initIOperatorExecutor = _ =>
      new HashJoinOpExec[K](
        getBuildTableLinkId(),
        buildAttributeName,
        probeAttributeName,
        joinType,
        operatorSchemaInfo
      )
  }

  override def isInputBlocking(input: LinkIdentity): Boolean = {
    input == getBuildTableLinkId()
  }

  override def getInputProcessingOrder(): Array[LinkIdentity] =
    Array(
      inputToOrdinalMapping.find(pair => pair._2._1 == 0).get._1,
      inputToOrdinalMapping.find(pair => pair._2._1 == 1).get._1
    )

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = {
    if (layer == getBuildTableLinkId().from) {
      Array(operatorSchemaInfo.inputSchemas(0).getIndex(buildAttributeName))

    } else {
      Array(operatorSchemaInfo.inputSchemas(1).getIndex(probeAttributeName))
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
