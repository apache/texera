package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.ActorRef

object ControlMessage {

  final case class Start() extends WorkflowMessage

  final case class Pause() extends WorkflowMessage

  final case class ModifyLogic(newMetadata: OpExecConfig) extends WorkflowMessage

  final case class Resume() extends WorkflowMessage

  final case class QueryState() extends WorkflowMessage

  final case class QueryStatistics() extends WorkflowMessage

  final case class CollectSinkResults() extends WorkflowMessage

  final case class LocalBreakpointTriggered() extends WorkflowMessage

  final case class RequireAck(msg: Any) extends WorkflowMessage

  final case class Ack() extends WorkflowMessage

  final case class AckWithInformation(info: Any) extends WorkflowMessage

  final case class AckWithSequenceNumber(sequenceNumber: Long) extends WorkflowMessage

  final case class AckOfEndSending() extends WorkflowMessage

  final case class StashOutput() extends WorkflowMessage

  final case class ReleaseOutput() extends WorkflowMessage

  final case class SkipTuple(faultedTuple: FaultedTuple) extends WorkflowMessage

  final case class SkipTupleGivenWorkerRef(actorPath: String, faultedTuple: FaultedTuple)
      extends WorkflowMessage

  final case class ModifyTuple(faultedTuple: FaultedTuple) extends WorkflowMessage

  final case class ResumeTuple(faultedTuple: FaultedTuple) extends WorkflowMessage

  final case class KillAndRecover() extends WorkflowMessage
}
