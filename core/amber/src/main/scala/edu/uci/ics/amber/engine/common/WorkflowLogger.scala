package edu.uci.ics.amber.engine.common

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.error.WorkflowRuntimeError

case class WorkflowLogger(private val logAction: WorkflowRuntimeError => Unit) {

  def log(err: WorkflowRuntimeError): Unit = {
    logAction.apply(err)
  }
}
