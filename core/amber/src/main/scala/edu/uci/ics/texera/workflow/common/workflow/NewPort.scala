package edu.uci.ics.texera.workflow.common.workflow


case class PortIdentity(
    id: Int,
    internal: Boolean = false
)
case object NewInputPort {
  def default: NewInputPort = {
    NewInputPort(PortIdentity(0))
  }


}
case class NewInputPort(
    id: PortIdentity,
    name: String = "",
    allowMultipleLinks: Boolean = false
)

case object NewOutputPort {
  def default: NewOutputPort = {
    NewOutputPort(PortIdentity(0))
  }

}
case class NewOutputPort(
    id: PortIdentity,
    name: String = ""
)
