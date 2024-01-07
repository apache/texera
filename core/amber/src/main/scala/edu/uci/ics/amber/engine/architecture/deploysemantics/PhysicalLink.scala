package edu.uci.ics.amber.engine.architecture.deploysemantics

import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalLinkIdentity

object PhysicalLink {
  def apply(
      fromPhysicalOp: PhysicalOp,
      fromPort: Int,
      toPhysicalOp: PhysicalOp,
      inputPort: Int
  ): PhysicalLink = {
    new PhysicalLink(
      fromPhysicalOp,
      fromPort,
      toPhysicalOp,
      inputPort
    )
  }
}
class PhysicalLink(
    @transient
    val fromOp: PhysicalOp,
    val fromPort: Int,
    @transient
    val toOp: PhysicalOp,
    val toPort: Int
) extends Serializable {

  val id: PhysicalLinkIdentity = PhysicalLinkIdentity(fromOp.id, fromPort, toOp.id, toPort)

}
