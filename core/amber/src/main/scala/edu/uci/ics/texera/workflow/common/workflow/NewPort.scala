package edu.uci.ics.texera.workflow.common.workflow

case class PortIdentity(
    id: Int
                       )
case object NewInputPort {
  def default: NewInputPort = {
    NewInputPort(PortIdentity(0))
  }
}
case class NewInputPort(
    id: PortIdentity
)

case object NewOutputPort {
  def default: NewOutputPort = {
    NewOutputPort(PortIdentity(0))
  }
}
case class NewOutputPort(
    id: PortIdentity
)
