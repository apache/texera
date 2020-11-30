package edu.uci.ics.amber.engine.common.error

/**
  *
  * @param name a descriptive name of the error
  * @param details some details about the error (for exception - its stacktrace)
  * @param errorType
  */
case class EngineError (name: String, details:String, errorType: EngineErrorType.Value)
