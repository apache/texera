package edu.uci.ics.texera.workflow.common.operators
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class HashOpExecConfig(
    override val id: OperatorIdentity,
    override val opExec: Int => OperatorExecutor
) extends OneToOneOpExecConfig(id, opExec) {

  override def requiredShuffle: Boolean = true

  override def getShuffleHashFunction(layer: LayerIdentity): ITuple => Int =
    (t: ITuple) => t.asInstanceOf[Tuple].hashCode()

}
