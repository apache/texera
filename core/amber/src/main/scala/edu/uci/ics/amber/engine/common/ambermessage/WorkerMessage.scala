package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataTransferPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.architecture.worker.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, LinkTag, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorPath, ActorRef}

import scala.collection.mutable

object WorkerMessage {

  final case class AckedWorkerInitialization(recoveryInformation: Seq[(Long, Long)] = Nil)
      extends WorkflowMessage

  final case class UpdateInputLinking(inputActor: ActorRef, fromLayer: LayerTag, inputNum: Int)
      extends WorkflowMessage

  final case class UpdateOutputLinking(
      policy: DataTransferPolicy,
      link: LinkTag,
      receivers: Array[BaseRoutee]
  ) extends WorkflowMessage

  final case class EndSending(sequenceNumber: Long) extends WorkflowMessage

  final case class ExecutionCompleted() extends WorkflowMessage

  final case class ExecutionPaused() extends WorkflowMessage

  final case class AssignBreakpoint(breakpoint: LocalBreakpoint) extends WorkflowMessage

  final case class QueryTriggeredBreakpoints() extends WorkflowMessage

  final case class QueryBreakpoint(id: String) extends WorkflowMessage

  final case class ReportState(workerState: WorkerState.Value) extends WorkflowMessage

  final case class ReportStatistics(workerStatistics: WorkerStatistics) extends WorkflowMessage

  final case class ReportOutputResult(results: List[ITuple]) extends WorkflowMessage

  final case class RemoveBreakpoint(id: String) extends WorkflowMessage

  final case class ReportedTriggeredBreakpoints(bps: Array[LocalBreakpoint]) extends WorkflowMessage

  final case class ReportedQueriedBreakpoint(bp: LocalBreakpoint) extends WorkflowMessage

  final case class ReportFailure(exception: Exception) extends WorkflowMessage

  final case class ReportUpstreamExhausted(tag: LayerTag) extends WorkflowMessage

  final case class ReportWorkerPartialCompleted(worker: WorkerTag, layer: LayerTag)
      extends WorkflowMessage

  final case class CheckRecovery() extends WorkflowMessage

  final case class ReportCurrentProcessingTuple(workerID: ActorPath, tuple: ITuple)
      extends WorkflowMessage

  final case class Reset(core: Any, recoveryInformation: Seq[(Long, Long)]) extends WorkflowMessage

  final case class DataMessage(sequenceNumber: Long, payload: Array[ITuple])
      extends WorkflowMessage {
    override def equals(obj: Any): Boolean = {
      if (!obj.isInstanceOf[DataMessage]) return false
      val other = obj.asInstanceOf[DataMessage]
      if (other eq null) return false
      if (sequenceNumber != other.sequenceNumber) {
        return false
      }
      if (payload.length != other.payload.length) {
        return false
      }
      var i = 0
      while (i < payload.length) {
        if (payload(i) != other.payload(i)) {
          return false
        }
        i += 1
      }
      true
    }
  }
}
