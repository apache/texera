package edu.uci.ics.texera.service.util

import edu.uci.ics.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import edu.uci.ics.texera.service.resource.ComputingUnitState.{ComputingUnitState, Pending, Running}

object ComputingUnitHelpers {
  def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState = {
    unit.getType match {
      // ── Local CUs are always “running” ──────────────────────────────
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // ── Kubernetes CUs – only explicit “Running” counts as running ─
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val phaseOpt = KubernetesClient
          .getPodByName(KubernetesClient.generatePodName(unit.getCuid))
          .map(_.getStatus.getPhase)

        if (phaseOpt.contains("Running")) Running else Pending

      // ── Any other (unknown) type is treated as pending ──────────────
      case _ =>
        Pending
    }
  }

  def getComputingUnitMetrics(unit: WorkflowComputingUnit): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = KubernetesClient.getPodMetrics(unit.getCuid)
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }
}
