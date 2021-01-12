package edu.uci.ics.amber.engine.common

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.error.WorkflowRuntimeError

case class WorkflowLogger(name: String) {

  private var logger = Logger(name)
  private var errorLogAction: WorkflowRuntimeError => Unit = null
  private var infoLogAction: String => Unit = null
  private var warningLogAction: String => Unit = null

  def setErrorLogAction(logAction: WorkflowRuntimeError => Unit) {
    errorLogAction = logAction
  }

  def setInfoLogAction(logAction: String => Unit) {
    infoLogAction = logAction
  }

  def setWarningLogAction(logAction: String => Unit) {
    warningLogAction = logAction
  }

  def logError(err: WorkflowRuntimeError): Unit = {
    logger.error(err.convertToMap().mkString(" | "))
    if (errorLogAction != null) {
      errorLogAction.apply(err)
    }
  }

  def logInfo(info: String): Unit = {
    logger.info(info)
    if (infoLogAction != null) {
      infoLogAction.apply(info)
    }
  }

  def logWarning(warning: String): Unit = {
    logger.warn(warning)
    if (warningLogAction != null) {
      warningLogAction.apply(warning)
    }
  }

}
