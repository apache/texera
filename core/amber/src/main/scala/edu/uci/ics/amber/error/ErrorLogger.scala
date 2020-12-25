package edu.uci.ics.amber.error

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.LogErrorToFrontEnd

case class ErrorLogger(name: String, logAction: WorkflowRuntimeError => Unit) {
  private val logger = Logger(name)

  def logToConsole(err: WorkflowRuntimeError): Unit = {
    logger.error(err.convertToMap().mkString(" | "))
  }

  // Use this in all actors except Controller. In Controller, directly call the
  // eventListener.workflowExecutionErrorListener and pass the error to it.
  def sendErrToFrontend(controllerRef: ActorRef, err: WorkflowRuntimeError): Unit = {
    controllerRef ! LogErrorToFrontEnd(err)
  }
}
