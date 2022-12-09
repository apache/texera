package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PythonConsoleMessageHandler.PythonConsoleMessage
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.PythonConsoleMessageV2
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.web.workflowruntimestate.{ConsoleMessage, OperatorRuntimeStats}

import scala.collection.mutable

object ControllerEvent {

  case class WorkflowCompleted() extends ControlCommand[Unit]

  case class WorkflowPaused() extends ControlCommand[Unit]

  case class WorkflowRecoveryStatus(isRecovering: Boolean) extends ControlCommand[Unit]

  case class WorkflowStatusUpdate(
      operatorStatistics: Map[String, OperatorRuntimeStats]
  ) extends ControlCommand[Unit]

  case class BreakpointTriggered(
      report: mutable.HashMap[(ActorVirtualIdentity, FaultedTuple), Array[String]],
      operatorID: String = null
  ) extends ControlCommand[Unit]

  case class ConsoleMessageTriggered(
      operatorId: String,
      consoleMessage: ConsoleMessage
  ) extends ControlCommand[Unit]

  case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(ITuple, ActorVirtualIdentity)]
  ) extends ControlCommand[Unit]

  case class WorkerAssignmentUpdate(workerMapping: Map[String, Seq[String]])
      extends ControlCommand[Unit]
}
