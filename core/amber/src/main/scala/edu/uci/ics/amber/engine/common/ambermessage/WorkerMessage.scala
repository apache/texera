package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataTransferPolicy
import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.architecture.worker.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.amberexception.AmberException
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, LinkTag, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorPath, ActorRef}

import scala.collection.mutable

object WorkerMessage {

  final case class AckedWorkerInitialization(recoveryInformation: Seq[(Long, Long)] = Nil)

  final case class UpdateInputLinking(inputActor: ActorRef, fromLayer: LayerTag)

  final case class UpdateOutputLinking(
      policy: DataTransferPolicy,
      link: LinkTag,
      receivers: Array[BaseRoutee]
  )

  final case class EndSending(sequenceNumber: Long)

  final case class ExecutionCompleted()

  final case class ExecutionPaused()

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

  final case class ReportUpstreamExhausted(tag: LayerTag)

  final case class ReportWorkerPartialCompleted(worker: WorkerTag, layer: LayerTag)

  final case class CheckRecovery()

  final case class ReportCurrentProcessingTuple(workerID: ActorPath, tuple: ITuple)

  final case class Reset(core: Any, recoveryInformation: Seq[(Long, Long)])

  final case class DataMessage(sequenceNumber: Long, payload: Array[ITuple]) {
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
