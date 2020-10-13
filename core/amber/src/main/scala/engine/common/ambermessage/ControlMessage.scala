package engine.common.ambermessage

import engine.operators.OpExecConfig
import engine.architecture.breakpoint.FaultedTuple
import engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import engine.common.tuple.Tuple
import akka.actor.ActorRef

object ControlMessage {

  final case class Start()

  final case class Pause()

  final case class ModifyLogic(newMetadata: OpExecConfig)

  final case class Resume()

  final case class QueryState()

  final case class QueryStatistics()

  final case class CollectSinkResults()

  final case class LocalBreakpointTriggered()

  final case class RequireAck(msg: Any)

  final case class Ack()

  final case class AckWithInformation(info: Any)

  final case class AckWithSequenceNumber(sequenceNumber: Long)

  final case class AckOfEndSending()

  final case class StashOutput()

  final case class ReleaseOutput()

  final case class SkipTuple(faultedTuple: FaultedTuple)

  final case class SkipTupleGivenWorkerRef(actorPath: String, faultedTuple: FaultedTuple)

  final case class ModifyTuple(faultedTuple: FaultedTuple)

  final case class ResumeTuple(faultedTuple: FaultedTuple)

  final case class KillAndRecover()
}
