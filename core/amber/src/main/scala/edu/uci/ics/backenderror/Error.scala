package edu.uci.ics.backenderror

/**
  * @param errorName a descriptive name of the error
  * @param errorSource where the error is occurring. eg: "Engine:Controller:CreateWorklow"
  * @param errorAdditionalParams details about the error: is this an unexpected exception or a constraint-violation, stacktrace etc.
  */
case class Error(errorName: String, errorSource:String, errorAdditionalParams: Map[String, String]) {

  def convertToMap(): Map[String,String] = {
    Map("errorName"->errorName, "errorSource"->errorSource, "errorSource"->errorSource.toString) ++ errorAdditionalParams
  }
}
