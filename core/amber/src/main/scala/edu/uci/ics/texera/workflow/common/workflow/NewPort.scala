package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OutputPort}

case class PortIdentity(
    id: Int,
    internal: Boolean = false
)
case object NewInputPort {
  def default: NewInputPort = {
    NewInputPort(PortIdentity(0))
  }

  def toNewInputPorts(oldInputPorts: List[InputPort]): List[NewInputPort] = {
    oldInputPorts.indices.toList.map(idx => NewInputPort(PortIdentity(idx)))
  }
}
case class NewInputPort(
    id: PortIdentity
)

case object NewOutputPort {
  def default: NewOutputPort = {
    NewOutputPort(PortIdentity(0))
  }

  def toNewOutputPorts(oldOutputPorts: List[OutputPort]): List[NewOutputPort] = {
    oldOutputPorts.indices.toList.map(idx => NewOutputPort(PortIdentity(idx)))
  }
}
case class NewOutputPort(
    id: PortIdentity
)
