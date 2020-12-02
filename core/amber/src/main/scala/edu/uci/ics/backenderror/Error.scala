package edu.uci.ics.backenderror

/**
  * @param errorName a descriptive name of the error
  * @param errorAdditionalDetails some details about the error (eg: exception's stacktrace)
  * @param errorType
  */
case class Error(errorName: String, errorType: ErrorType.Value, errorSource:ErrorSource.Value, errorAdditionalDetails: Map[String, String]) {

  def convertToMap(): Map[String,String] = {
    Map("errorName"->errorName, "errorType"->errorType.toString, "errorSource"->errorSource.toString) ++ errorAdditionalDetails
  }
}
