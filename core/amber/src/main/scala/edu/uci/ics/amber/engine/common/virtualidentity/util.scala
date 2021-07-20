package edu.uci.ics.amber.engine.common.virtualidentity

object util {

  lazy val CONTROLLER: ActorVirtualIdentity = ActorVirtualIdentity("CONTROLLER")
  lazy val SELF: ActorVirtualIdentity = ActorVirtualIdentity("SELF")

  def makeLayer(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, layerID)
  }

  def toOperatorIdentity(layerIdentity: Option[LayerIdentity]): OperatorIdentity =
    OperatorIdentity(layerIdentity.get.workflow, layerIdentity.get.operator)
}
