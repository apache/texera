package edu.uci.ics.texera.workflow.common.operators
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class HashOpExecConfig(
    override val id: OperatorIdentity,
    override val opExec: Int => OperatorExecutor,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OneToOneOpExecConfig(id, opExec) {

  override def requiredShuffle: Boolean = true

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] =
    operatorSchemaInfo.inputSchemas(0).getAttributes.toArray.indices.toArray

}
