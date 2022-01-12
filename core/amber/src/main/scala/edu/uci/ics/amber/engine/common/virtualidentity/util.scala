package edu.uci.ics.amber.engine.common.virtualidentity

object util {

  lazy val CONTROLLER: ActorVirtualIdentity = ActorVirtualIdentity("CONTROLLER")
  lazy val SELF: ActorVirtualIdentity = ActorVirtualIdentity("SELF")
  lazy val CLIENT: ActorVirtualIdentity = ActorVirtualIdentity("CLIENT")

  def makeLayer(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, layerID)
  }

  def toOperatorIdentity(layerIdentity: LayerIdentity): OperatorIdentity =
    OperatorIdentity(layerIdentity.workflow, layerIdentity.operator)
}
