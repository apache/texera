package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.web.workflowruntimestate.OperatorRuntimeStats

object ControllerEvent {

  case class WorkflowCompleted() extends ControlCommand[Unit]

  case class WorkflowPaused() extends ControlCommand[Unit]

  case class WorkflowStatsUpdate(
      operatorStatistics: Map[String, OperatorRuntimeStats]
  ) extends ControlCommand[Unit]

  case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(ITuple, ActorVirtualIdentity)]
  ) extends ControlCommand[Unit]

  case class WorkerAssignmentUpdate(workerMapping: Map[String, Seq[String]])
      extends ControlCommand[Unit]
}
