package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.config.{OperatorConfig, WorkerConfig}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.COMPLETED
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.workflow.LogicalLink
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowSchedulerSpec extends AnyFlatSpec with MockFactory {

  def setOperatorCompleted(
      workflow: Workflow,
      executionState: ExecutionState,
      logicalOpId: OperatorIdentity
  ): Unit = {
    val physicalOps = workflow.physicalPlan.getPhysicalOpsOfLogicalOp(logicalOpId)
    physicalOps.foreach { physicalOp =>
      executionState.initOperatorState(
        physicalOp.id,
        OperatorConfig(List(WorkerConfig(workerId = null)))
      )
      executionState.getOperatorExecution(physicalOp.id).setAllWorkerState(COMPLETED)
    }
  }

  "Scheduler" should "correctly schedule regions in headerlessCsv->keyword->sink workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )
    val executionState = new ExecutionState(workflow)
    val scheduler =
      new WorkflowScheduler(
        workflow.regionPlan.regions.toBuffer,
        executionState,
        ControllerConfig.default,
        null
      )
    Set(
      headerlessCsvOpDesc.operatorIdentifier,
      keywordOpDesc.operatorIdentifier,
      sink.operatorIdentifier
    ).foreach(logicalOpId => setOperatorCompleted(workflow, executionState, logicalOpId))
    scheduler.schedulingPolicy.addToRunningRegions(
      scheduler.schedulingPolicy.startWorkflow(workflow),
      null
    )
    val csvPhysicalOpId = workflow.physicalPlan
      .getPhysicalOpsOfLogicalOp(headerlessCsvOpDesc.operatorIdentifier)
      .head
      .id
    val keywordPhysicalOpId =
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(keywordOpDesc.operatorIdentifier).head.id
    val sinkPhysicalOpId =
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(sink.operatorIdentifier).head.id
    var nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(
        workflow,
        executionState,
        VirtualIdentityUtils.createWorkerIdentity(
          workflow.context.workflowId,
          csvPhysicalOpId,
          0
        )
      )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.isEmpty)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(csvPhysicalOpId, PortIdentity(), keywordPhysicalOpId, PortIdentity())
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.isEmpty)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(keywordPhysicalOpId, PortIdentity(), sinkPhysicalOpId, PortIdentity())
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 1)

  }

  "Scheduler" should "correctly schedule regions in buildcsv->probecsv->hashjoin->hashjoin->sink workflow" in {
    val buildCsv = TestOperators.headerlessSmallCsvScanOpDesc()
    val probeCsv = TestOperators.smallCsvScanOpDesc()
    val hashJoin1 = TestOperators.joinOpDesc("column-1", "Region")
    val hashJoin2 = TestOperators.joinOpDesc("column-2", "Country")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        buildCsv,
        probeCsv,
        hashJoin1,
        hashJoin2,
        sink
      ),
      List(
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          probeCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          hashJoin1.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          hashJoin2.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )

    val executionState = new ExecutionState(workflow)
    val scheduler =
      new WorkflowScheduler(
        workflow.regionPlan.regions.toBuffer,
        executionState,
        ControllerConfig.default,
        null
      )
    scheduler.schedulingPolicy.addToRunningRegions(
      scheduler.schedulingPolicy.startWorkflow(workflow),
      null
    )
    Set(buildCsv.operatorIdentifier).foreach(logicalOpId =>
      setOperatorCompleted(workflow, executionState, logicalOpId)
    )
    val opIdentity = buildCsv.operatorIdentifier
    val physicalOpId = workflow.physicalPlan.getPhysicalOpsOfLogicalOp(opIdentity).head.id
    var nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(
        workflow,
        executionState,
        VirtualIdentityUtils.createWorkerIdentity(
          workflow.context.workflowId,
          physicalOpId,
          0
        )
      )
    assert(nextRegions.isEmpty)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            buildCsv.operatorIdentifier
          )
          .last
          .id,
        PortIdentity(),
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin1.operatorIdentifier
          )
          .head
          .id,
        PortIdentity()
      )
    )
    assert(nextRegions.isEmpty)
    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            buildCsv.operatorIdentifier
          )
          .last
          .id,
        PortIdentity(),
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin2.operatorIdentifier
          )
          .head
          .id,
        PortIdentity()
      )
    )
    assert(nextRegions.nonEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 1)
    scheduler.schedulingPolicy.addToRunningRegions(nextRegions, null)
    Set(
      probeCsv.operatorIdentifier,
      hashJoin1.operatorIdentifier,
      hashJoin2.operatorIdentifier,
      sink.operatorIdentifier
    ).foreach(logicalOpId => setOperatorCompleted(workflow, executionState, logicalOpId))
    val probeLogicalOpId = probeCsv.operatorIdentifier
    val probePhysicalOpId =
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(probeLogicalOpId).head.id
    nextRegions = scheduler.schedulingPolicy.onWorkerCompletion(
      workflow,
      executionState,
      VirtualIdentityUtils.createWorkerIdentity(
        workflow.context.workflowId,
        probePhysicalOpId,
        0
      )
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 1)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            probeCsv.operatorIdentifier
          )
          .head
          .id,
        PortIdentity(),
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin1.operatorIdentifier
          )
          .head
          .id,
        PortIdentity(1)
      )
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 1)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin1.operatorIdentifier
          )
          .head
          .id,
        PortIdentity(),
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin2.operatorIdentifier
          )
          .head
          .id,
        PortIdentity(1)
      )
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 1)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      PhysicalLink(
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            hashJoin2.operatorIdentifier
          )
          .head
          .id,
        PortIdentity(),
        workflow.physicalPlan
          .getPhysicalOpsOfLogicalOp(
            sink.operatorIdentifier
          )
          .head
          .id,
        PortIdentity()
      )
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions.size == 2)
  }

}
