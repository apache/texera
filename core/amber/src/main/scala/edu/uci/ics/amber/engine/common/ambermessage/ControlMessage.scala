package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.ActorRef
import edu.uci.ics.amber.engine.common.ambermessage.neo.ControlPayload
import edu.uci.ics.amber.error.WorkflowRuntimeError

object ControlMessage {

  final case class Start() extends ControlPayload

  final case class Pause() extends ControlPayload

  final case class ModifyLogic(newMetadata: OpExecConfig) extends ControlPayload

  final case class Resume() extends ControlPayload

  final case class QueryState() extends ControlPayload

  final case class QueryStatistics() extends ControlPayload

  final case class CollectSinkResults() extends ControlPayload

  final case class LocalBreakpointTriggered() extends ControlPayload

  final case class RequireAck(msg: Any) extends ControlPayload

  final case class Ack() extends ControlPayload

  final case class AckWithInformation(info: Any) extends ControlPayload

  final case class AckWithSequenceNumber(sequenceNumber: Long) extends ControlPayload

  final case class AckOfEndSending() extends ControlPayload

  final case class StashOutput() extends ControlPayload

  final case class ReleaseOutput() extends ControlPayload

  final case class SkipTuple(faultedTuple: FaultedTuple) extends ControlPayload

  final case class SkipTupleGivenWorkerRef(actorPath: String, faultedTuple: FaultedTuple)
      extends ControlPayload

  final case class ModifyTuple(faultedTuple: FaultedTuple) extends ControlPayload

  final case class ResumeTuple(faultedTuple: FaultedTuple) extends ControlPayload

  final case class KillAndRecover() extends ControlPayload

  final case class LogErrorToFrontEnd(err: WorkflowRuntimeError) extends ControlPayload
}
