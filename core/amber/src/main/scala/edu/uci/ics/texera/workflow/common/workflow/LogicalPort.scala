package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

case object LogicalPort {
  def apply(operatorIdentity: OperatorIdentity, portOrdinal: Integer): LogicalPort = {
    LogicalPort(operatorIdentity.id, portOrdinal)
  }
  def apply(
      operatorIdentity: OperatorIdentity,
      portOrdinal: Integer,
      portName: String
  ): LogicalPort = {
    LogicalPort(operatorIdentity.id, portOrdinal, portName)
  }
}
case class LogicalPort(
    operatorID: String,
    portOrdinal: Integer = 0,
    portName: String = ""
) {
  def operatorId: OperatorIdentity = OperatorIdentity(operatorID)
}
