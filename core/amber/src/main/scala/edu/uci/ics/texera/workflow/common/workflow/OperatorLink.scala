package edu.uci.ics.texera.workflow.common.workflow

case class OperatorPort(operatorID: String, portOrdinal: Integer)

case class OperatorLink(origin: OperatorPort, destination: OperatorPort)
