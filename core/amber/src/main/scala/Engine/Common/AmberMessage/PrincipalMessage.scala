package Engine.Common.AmberMessage

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Principal.{PrincipalState, PrincipalStatistics}
import Engine.Common.AmberTag.{AmberTag, LayerTag, WorkerTag}
import Engine.Common.tuple.Tuple
import Engine.Operators.OpExecConfig
import akka.actor.{ActorPath, ActorRef}

import scala.collection.mutable

object PrincipalMessage {
  final case class AckedPrincipalInitialization(prev: Array[(OpExecConfig, ActorLayer)])

  final case class GetInputLayer()

  final case class GetOutputLayer()

  final case class AppendLayer(linkStrategy: LinkStrategy)

  final case class PrependLayer(
                                 prev: Array[(OpExecConfig, ActorLayer)],
                                 linkStrategy: LinkStrategy
  )

  final case class AssignBreakpoint(breakpoint: GlobalBreakpoint)

  final case class ReportState(principalState: PrincipalState.Value)

  final case class ReportStatistics(principalStatistics: PrincipalStatistics)

  final case class ReportOutputResult(results: List[Tuple])

  final case class ReportPrincipalPartialCompleted(from: AmberTag, layer: LayerTag)

  final case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(Tuple, ActorPath)]
  )

}
