package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.principal.{PrincipalState, PrincipalStatistics}
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.operators.OpExecConfig
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

  final case class ReportOutputResult(results: List[ITuple])

  final case class ReportPrincipalPartialCompleted(from: AmberTag, layer: LayerTag)

  final case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(ITuple, ActorPath)]
  )

}
