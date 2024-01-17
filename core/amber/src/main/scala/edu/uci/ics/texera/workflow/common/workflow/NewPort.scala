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
    oldInputPorts.zipWithIndex.map {
      case (port, idx) => NewInputPort(PortIdentity(idx), name = port.displayName, allowMultipleLinks = port.allowMultiInputs)
    }
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

  def toNewOutputPorts(oldOutputPorts: List[OutputPort]): List[NewOutputPort] = {
    oldOutputPorts.zipWithIndex.map {
      case (port, idx) => NewOutputPort(PortIdentity(idx), name = port.displayName)
    }
  }
}
case class NewOutputPort(
    id: PortIdentity,
    name: String = ""
)
