package edu.uci.ics.amber.engine.common.virtualidentity

object util {

  lazy val CONTROLLER: ActorVirtualIdentity = ActorVirtualIdentity("CONTROLLER")
  lazy val SELF: ActorVirtualIdentity = ActorVirtualIdentity("SELF")
  lazy val CLIENT: ActorVirtualIdentity = ActorVirtualIdentity("CLIENT")

  lazy val SOURCE_STARTER_ACTOR: ActorVirtualIdentity = ActorVirtualIdentity("SOURCE_STARTER")
  lazy val SOURCE_STARTER_OP: LayerIdentity =
    LayerIdentity("SOURCE_STARTER", "SOURCE_STARTER")

  def makeLayer(operatorIdentity: OperatorIdentity, layerID: String): LayerIdentity = {
    LayerIdentity(operatorIdentity.id, layerID)
  }

  def toOperatorIdentity(layerIdentity: LayerIdentity): OperatorIdentity =
    OperatorIdentity(layerIdentity.operator)
}
