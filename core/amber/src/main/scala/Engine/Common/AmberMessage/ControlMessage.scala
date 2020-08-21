package Engine.Common.AmberMessage

import Engine.Operators.OperatorMetadata
import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.LocalBreakpoint
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef


object ControlMessage{

  final case class Start()

  final case class Pause()

  final case class ModifyLogic(newMetadata:OperatorMetadata)

  final case class Resume()

  final case class QueryState()

  final case class QueryStatistics()

  final case class CollectSinkResults()

  final case class LocalBreakpointTriggered()

  final case class RequireAck(msg:Any)

  final case class Ack()

  final case class AckWithInformation(info:Any)

  final case class AckWithSequenceNumber(sequenceNumber: Long)

  final case class AckOfEndSending()

  final case class StashOutput()

  final case class ReleaseOutput()

  final case class SkipTuple(faultedTuple:FaultedTuple)

  final case class SkipTupleGivenWorkerRef(actorPath: String, faultedTuple: FaultedTuple)

  final case class ModifyTuple(faultedTuple:FaultedTuple)

  final case class ResumeTuple(faultedTuple:FaultedTuple)

  final case class KillAndRecover()
}
