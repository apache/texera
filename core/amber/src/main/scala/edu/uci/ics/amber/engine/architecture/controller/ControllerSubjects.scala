package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent._
import edu.uci.ics.texera.web.resource.{Observable, Subject}

case class ControllerSubjects(){
  val workflowCompleted = Subject[WorkflowCompleted]()
  val workflowStatusUpdate = Subject[WorkflowStatusUpdate]()
  val workflowResultUpdate = Subject[WorkflowResultUpdate]()
  val breakpointTriggered = Subject[BreakpointTriggered]()
  val workflowPaused = Subject[WorkflowPaused]()
  val reportCurrentTuples = Subject[ReportCurrentProcessingTuple]()
  val workflowExecutionError = Subject[ErrorOccurred]()
  val pythonPrintTriggered = Subject[PythonPrintTriggered]()
}
