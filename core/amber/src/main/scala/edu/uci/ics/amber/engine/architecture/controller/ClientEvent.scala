package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessagePayload
import edu.uci.ics.amber.engine.common.executionruntimestate.OperatorMetrics
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

trait ClientEvent extends WorkflowFIFOMessagePayload

case class ExecutionStateUpdate(state: WorkflowAggregatedState) extends ClientEvent

case class ExecutionStatsUpdate(operatorMetrics: Map[String, OperatorMetrics]) extends ClientEvent

case class ReportCurrentProcessingTuple(
    operatorID: String,
    tuple: Array[(Tuple, ActorVirtualIdentity)]
) extends ClientEvent

case class WorkerAssignmentUpdate(workerMapping: Map[String, Seq[String]]) extends ClientEvent

final case class FatalError(e: Throwable, fromActor: Option[ActorVirtualIdentity] = None)
    extends ClientEvent

case class UpdateExecutorCompleted(id: ActorVirtualIdentity) extends ClientEvent

final case class ReplayStatusUpdate(id: ActorVirtualIdentity, status: Boolean) extends ClientEvent

final case class WorkflowRecoveryStatus(isRecovering: Boolean) extends ClientEvent
