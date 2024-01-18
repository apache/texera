package edu.uci.ics.texera.workflow.common.workflow


case class PortIdentity(
    id: Int,
    internal: Boolean = false
)
case object InputPort {
  def default: InputPort = {
    InputPort(PortIdentity(0))
  }


}
case class InputPort(
    id: PortIdentity,
    name: String = "",
    allowMultipleLinks: Boolean = false
)

case object OutputPort {
  def default: OutputPort = {
    OutputPort(PortIdentity(0))
  }

}
case class OutputPort(
    id: PortIdentity,
    name: String = ""
)
