package edu.uci.ics.texera.workflow.common.operators
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.ShuffleType

class HashOpExecConfig(
    override val id: OperatorIdentity,
    override val opExec: Int => OperatorExecutor,
    hashColumnIndices: Array[Int]
) extends OneToOneOpExecConfig(id, opExec) {

  shuffleType = ShuffleType.HASH_BASED

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = hashColumnIndices

}
