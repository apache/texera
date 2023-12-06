package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.COMPLETED
import edu.uci.ics.amber.engine.common.{VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.Utils.buildWorkflow
import edu.uci.ics.texera.workflow.common.workflow.{OperatorLink, OperatorPort}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowSchedulerSpec extends AnyFlatSpec with MockFactory {

  def setOperatorCompleted(
      workflow: Workflow,
      executionState: ExecutionState,
      opID: String
  ): Unit = {
    val opIdentity = new OperatorIdentity(workflow.workflowId.id, opID)
    val layers = workflow.physicalPlan.layersOfLogicalOperator(opIdentity)
    layers.foreach { layer =>
      executionState.getOperatorExecution(layer.id).setAllWorkerState(COMPLETED)
    }
  }

  "Scheduler" should "correctly schedule regions in headerlessCsv->keyword->sink workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc, sink),
      List(
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(keywordOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
    val executionState = new ExecutionState(workflow)
    val scheduler =
      new WorkflowScheduler(
        workflow.physicalPlan.regionsToSchedule.toBuffer,
        executionState,
        ControllerConfig.default,
        null
      )
    Set(headerlessCsvOpDesc.operatorID, keywordOpDesc.operatorID, sink.operatorID).foreach(opID =>
      setOperatorCompleted(workflow, executionState, opID)
    )
    scheduler.schedulingPolicy.addToRunningRegions(
      scheduler.schedulingPolicy.startWorkflow(workflow),
      null
    )
    val opIdentity = new OperatorIdentity(workflow.workflowId.id, headerlessCsvOpDesc.operatorID)
    val layerId = workflow.physicalPlan.layersOfLogicalOperator(opIdentity).head.id
    val nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(
        workflow,
        executionState,
        VirtualIdentityUtils.createWorkerIdentity(layerId, 0)
      )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions().size == 1)
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
        OperatorLink(
          OperatorPort(buildCsv.operatorID, 0),
          OperatorPort(hashJoin1.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(probeCsv.operatorID, 0),
          OperatorPort(hashJoin1.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(buildCsv.operatorID, 0),
          OperatorPort(hashJoin2.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(hashJoin1.operatorID, 0),
          OperatorPort(hashJoin2.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(hashJoin2.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
    val executionState = new ExecutionState(workflow)
    val scheduler =
      new WorkflowScheduler(
        workflow.physicalPlan.regionsToSchedule.toBuffer,
        executionState,
        ControllerConfig.default,
        null
      )
    scheduler.schedulingPolicy.addToRunningRegions(
      scheduler.schedulingPolicy.startWorkflow(workflow),
      null
    )
    Set(buildCsv.operatorID).foreach(opID => setOperatorCompleted(workflow, executionState, opID))
    val opIdentity = new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID)
    val layerId = workflow.physicalPlan.layersOfLogicalOperator(opIdentity).head.id
    var nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(
        workflow,
        executionState,
        VirtualIdentityUtils.createWorkerIdentity(layerId, 0)
      )
    assert(nextRegions.isEmpty)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      LinkIdentity(
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID)
          )
          .last
          .id,
        0,
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, hashJoin1.operatorID)
          )
          .head
          .id,
        0
      )
    )
    assert(nextRegions.isEmpty)
    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      workflow,
      executionState,
      LinkIdentity(
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID)
          )
          .last
          .id,
        0,
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, hashJoin2.operatorID)
          )
          .head
          .id,
        0
      )
    )
    assert(nextRegions.nonEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions().size == 1)
    scheduler.schedulingPolicy.addToRunningRegions(nextRegions, null)
    Set(probeCsv.operatorID, hashJoin1.operatorID, hashJoin2.operatorID, sink.operatorID).foreach(
      opID => setOperatorCompleted(workflow, executionState, opID)
    )
    val probeId = new OperatorIdentity(workflow.workflowId.id, probeCsv.operatorID)
    val probeLayerId = workflow.physicalPlan.layersOfLogicalOperator(probeId).head.id
    nextRegions = scheduler.schedulingPolicy.onWorkerCompletion(
      workflow,
      executionState,
      VirtualIdentityUtils.createWorkerIdentity(probeLayerId, 0)
    )
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions().size == 2)
  }

}
