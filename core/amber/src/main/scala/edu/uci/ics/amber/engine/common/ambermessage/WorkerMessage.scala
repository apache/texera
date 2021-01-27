package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.architecture.worker.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, LinkTag, OperatorIdentifier, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorPath, ActorRef}
import edu.uci.ics.amber.engine.common.ambermessage.neo.{
  ControlPayload,
  DataPayload,
  WorkflowMessage
}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

object WorkerMessage {

  final case class AckedWorkerInitialization(recoveryInformation: Seq[(Long, Long)] = Nil)

  final case class UpdateInputLinking(
      upstreamWorkerId: VirtualIdentity,
      upstreamOpId: OperatorIdentifier
  ) extends ControlPayload

  final case class AddDataSendingPolicy(
      policy: DataSendingPolicy
  ) extends ControlPayload

  final case class EndSending(sequenceNumber: Long)

  final case class ExecutionCompleted() extends ControlPayload

  final case class ExecutionPaused()

  final case class CurrentLoadMetrics(
      unprocessedQueueLength: Long,
      totalPutInInternalQueue: Long,
      stashedBatches: Long
  ) // join-skew research related. All lengths are in batch counts

  final case class FutureLoadMetrics(dataToSend: Map[ActorVirtualIdentity, Long])

  final case class AssignBreakpoint(breakpoint: LocalBreakpoint)

  final case class QueryTriggeredBreakpoints()

  final case class QueryBreakpoint(id: String)

  final case class ReportState(workerState: WorkerState.Value)

  final case class ReportStatistics(workerStatistics: WorkerStatistics)

  final case class ReportOutputResult(results: List[ITuple])

  final case class RemoveBreakpoint(id: String)

  final case class ReportedTriggeredBreakpoints(bps: Array[LocalBreakpoint])

  final case class ReportedQueriedBreakpoint(bp: LocalBreakpoint)

  final case class ReportFailure(exception: Exception)

  final case class ReportUpstreamExhausted(inputOperatorRef: Int) extends ControlPayload

  final case class ReportWorkerPartialCompleted(inputOp: OperatorIdentifier) extends ControlPayload

  final case class CheckRecovery()

  final case class ReportCurrentProcessingTuple(workerID: ActorPath, tuple: ITuple)

  final case class Reset(core: Any, recoveryInformation: Seq[(Long, Long)])

  final case class EndOfUpstream() extends DataPayload

  final case class DataFrame(frame: Array[ITuple]) extends DataPayload {
    override def equals(obj: Any): Boolean = {
      if (!obj.isInstanceOf[DataFrame]) return false
      val other = obj.asInstanceOf[DataFrame]
      if (other eq null) return false
      if (frame.length != other.frame.length) {
        return false
      }
      var i = 0
      while (i < frame.length) {
        if (frame(i) != other.frame(i)) {
          return false
        }
        i += 1
      }
      true
    }
  }
}
