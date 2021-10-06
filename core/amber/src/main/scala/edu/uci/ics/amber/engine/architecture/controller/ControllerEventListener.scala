package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent._
import edu.uci.ics.texera.web.model.common.python.PythonExpressionEvaluateResponse

case class ControllerEventListener(
    var workflowCompletedListener: WorkflowCompleted => Unit = null,
    workflowStatusUpdateListener: WorkflowStatusUpdate => Unit = null,
    workflowResultUpdateListener: WorkflowResultUpdate => Unit = null,
    breakpointTriggeredListener: BreakpointTriggered => Unit = null,
    workflowPausedListener: WorkflowPaused => Unit = null,
    reportCurrentTuplesListener: ReportCurrentProcessingTuple => Unit = null,
    recoveryStartedListener: Unit => Unit = null,
    workflowExecutionErrorListener: ErrorOccurred => Unit = null,
    pythonPrintTriggeredListener: PythonPrintTriggered => Unit = null,
    pythonExpressionEvaluatedListener: PythonExpressionEvaluateResponse => Unit = null
)
