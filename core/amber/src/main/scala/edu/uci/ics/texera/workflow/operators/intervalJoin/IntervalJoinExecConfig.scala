package edu.uci.ics.texera.workflow.operators.intervalJoin

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

class IntervalJoinExecConfig(
    id: OperatorIdentity,
    val leftJoinKeyName: String,
    val rightJoinkeyName: String,
    val operatorSchemaInfo: OperatorSchemaInfo,
    var desc: IntervalJoinOpDesc
) extends OpExecConfig(id) {
  shuffleType = ShuffleType.HASH_BASED

  def getLeftInputLink(): LinkIdentity = inputToOrdinalMapping.find(pair => pair._2._1 == 0).get._1

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          _ =>
            new IntervalJoinOpExec(
              operatorSchemaInfo,
              desc,
              getLeftInputLink()
            ),
          1,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def isInputBlocking(input: LinkIdentity): Boolean = {
    input == getLeftInputLink()
  }

  override def getInputProcessingOrder(): Array[LinkIdentity] =
    Array(
      inputToOrdinalMapping.find(pair => pair._2._1 == 0).get._1,
      inputToOrdinalMapping.find(pair => pair._2._1 == 1).get._1
    )

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = {
    if (layer == getLeftInputLink().from) {
      Array(operatorSchemaInfo.inputSchemas(0).getIndex(leftJoinKeyName))

    } else {
      Array(operatorSchemaInfo.inputSchemas(1).getIndex(rightJoinkeyName))
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
