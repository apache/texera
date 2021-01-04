package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.principal.{PrincipalState, PrincipalStatistics}
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{ActorPath, ActorRef}

import scala.collection.mutable

object PrincipalMessage {
  final case class AckedPrincipalInitialization(prev: Array[(OpExecConfig, ActorLayer)])
      extends WorkflowMessage

  final case class GetInputLayer() extends WorkflowMessage

  final case class GetOutputLayer() extends WorkflowMessage

  final case class AppendLayer(linkStrategy: LinkStrategy) extends WorkflowMessage

  final case class PrependLayer(
      prev: Array[(OpExecConfig, ActorLayer)],
      linkStrategy: LinkStrategy
  ) extends WorkflowMessage

  final case class AssignBreakpoint(breakpoint: GlobalBreakpoint) extends WorkflowMessage

  final case class ReportState(principalState: PrincipalState.Value) extends WorkflowMessage

  final case class ReportStatistics(principalStatistics: PrincipalStatistics)
      extends WorkflowMessage

  final case class ReportOutputResult(results: List[ITuple]) extends WorkflowMessage

  final case class ReportPrincipalPartialCompleted(from: AmberTag, layer: LayerTag)
      extends WorkflowMessage

  final case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(ITuple, ActorPath)]
  ) extends WorkflowMessage

}
