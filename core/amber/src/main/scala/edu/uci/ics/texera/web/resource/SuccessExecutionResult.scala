package edu.uci.ics.texera.web.resource

case class SuccessExecutionResult(
    resultID: String,
    code: Integer = 0,
    result: List[String] = List()
)
