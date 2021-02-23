package edu.uci.ics.amber.engine.common.virtualidentity

object LayerIdentity {
  def apply(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, layerID)
  }
}

case class LayerIdentity(workflow: String, operator: String, layerID: String)
    extends VirtualIdentity {
  override def toString: String = s"Layer($workflow,$operator,$layerID)"

  def toOperatorIdentity: OperatorIdentity = OperatorIdentity(workflow, operator)

}
