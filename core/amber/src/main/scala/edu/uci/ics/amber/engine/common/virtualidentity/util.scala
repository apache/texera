package edu.uci.ics.amber.engine.common.virtualidentity

object util {

  lazy val CONTROLLER: ControllerVirtualIdentity = ControllerVirtualIdentity()
  lazy val SELF: SelfVirtualIdentity = SelfVirtualIdentity()
  lazy val CLIENT: ClientVirtualIdentity = ClientVirtualIdentity()

  def makeLayer(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, layerID)
  }

  def toOperatorIdentity(layerIdentity: LayerIdentity): OperatorIdentity =
    OperatorIdentity(layerIdentity.workflow, layerIdentity.operator)
}
