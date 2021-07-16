package edu.uci.ics.texera.workflow.common.operators
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}

class HashOpExecConfig(
    override val id: OperatorIdentity,
    override val opExec: Int => OperatorExecutor,
    hashColumnIndices: Array[Int]
) extends OneToOneOpExecConfig(id, opExec) {

  override def requiredShuffle: Boolean = true

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = hashColumnIndices

}
