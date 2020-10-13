package engine.common.ambermessage

import engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import engine.architecture.deploysemantics.layer.ActorLayer
import engine.architecture.linksemantics.LinkStrategy
import engine.architecture.principal.{PrincipalState, PrincipalStatistics}
import engine.common.ambertag.{AmberTag, LayerTag, WorkerTag}
import engine.common.tuple.Tuple
import engine.operators.OpExecConfig
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
