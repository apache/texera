package edu.uci.ics.amber.error

object WorkflowRuntimeError {
  def apply(exception: Throwable, source: String): WorkflowRuntimeError = {
    WorkflowRuntimeError(
      exception.getMessage,
      source,
      Map("stackTrace" -> exception.getStackTrace.mkString("\n"))
    )
  }
}

/**
  * @param errorMessage a descriptive name of the error
  * @param errorSource where the error is occurring. eg: "Engine:CONTROLLER:CreateWorklow"
  * @param errorAdditionalParams details about the error: is this an unexpected exception or a constraint-violation, stacktrace etc.
  */
case class WorkflowRuntimeError(
    errorMessage: String,
    errorSource: String,
    errorAdditionalParams: Map[String, String]
) {

  def convertToMap(): Map[String, String] = {
    Map(
      "errorMessage" -> (if (errorMessage != null) errorMessage else ""),
      "errorSource" -> (if (errorSource != null) errorSource else "")
    ) ++ (if (errorAdditionalParams != null) errorAdditionalParams else Map())
  }
}
