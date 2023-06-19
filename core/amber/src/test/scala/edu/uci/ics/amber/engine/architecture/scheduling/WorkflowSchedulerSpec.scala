package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerInfo
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  UNINITIALIZED
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{
  LogicalPlan,
  OperatorLink,
  OperatorPort,
  WorkflowCompiler
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

class WorkflowSchedulerSpec extends AnyFlatSpec with MockFactory {

  def buildWorkflow(
      operators: List[OperatorDescriptor],
      links: List[OperatorLink]
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      LogicalPlan(operators, links, List()),
      context
    )
    texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity("workflow-test"), new OpResultStorage())
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

    val logger = Logger(
      LoggerFactory.getLogger(s"WorkflowSchedulerTest")
    )
    val scheduler =
      new WorkflowScheduler(Array(), null, null, null, logger, workflow, ControllerConfig.default)
    workflow.physicalPlan
      .layersOfLogicalOperator(
        new OperatorIdentity(workflow.workflowId.id, headerlessCsvOpDesc.operatorID)
      )
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Scan worker $i")) = l
          ActorVirtualIdentity(s"Scan worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Scan worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(
        new OperatorIdentity(workflow.workflowId.id, keywordOpDesc.operatorID)
      )
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Keyword worker $i")) = l
          ActorVirtualIdentity(s"Keyword worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Keyword worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, sink.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Sink worker $i")) = l
          ActorVirtualIdentity(s"Sink worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Sink worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    Set(headerlessCsvOpDesc.operatorID, keywordOpDesc.operatorID, sink.operatorID).foreach(opID => {
      workflow.physicalPlan
        .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, opID))
        .foreach(l => {
          l.workers.keys.foreach(wid => {
            l.workers(wid).state = COMPLETED
          })
        })
    })
    scheduler.schedulingPolicy.addToRunningRegions(scheduler.schedulingPolicy.startWorkflow())
    val nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(ActorVirtualIdentity("Scan worker 0"))
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
    val logger = Logger(
      LoggerFactory.getLogger(s"WorkflowSchedulerTest")
    )
    val scheduler =
      new WorkflowScheduler(Array(), null, null, null, logger, workflow, ControllerConfig.default)

    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Build Scan worker $i")) = l
          ActorVirtualIdentity(s"Build Scan worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Build Scan worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, probeCsv.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Probe Scan worker $i")) = l
          ActorVirtualIdentity(s"Probe Scan worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Probe Scan worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, hashJoin1.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"HashJoin1 worker $i")) = l
          ActorVirtualIdentity(s"HashJoin1 worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"HashJoin1 worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, hashJoin2.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"HashJoin2 worker $i")) = l
          ActorVirtualIdentity(s"HashJoin2 worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"HashJoin2 worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })
    workflow.physicalPlan
      .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, sink.operatorID))
      .foreach(l => {
        l.workers = ListMap((0 until 1).map { i =>
          workflow.workerToOpExecConfig(ActorVirtualIdentity(s"Sink worker $i")) = l
          ActorVirtualIdentity(s"Sink worker $i") -> WorkerInfo(
            ActorVirtualIdentity(s"Sink worker $i"),
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            null
          )
        }: _*)
      })

    scheduler.schedulingPolicy.addToRunningRegions(scheduler.schedulingPolicy.startWorkflow())
    Set(buildCsv.operatorID).foreach(opID => {
      workflow.physicalPlan
        .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, opID))
        .foreach(l => {
          l.workers.keys.foreach(wid => {
            l.workers(wid).state = COMPLETED
          })
        })
    })

    var nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(ActorVirtualIdentity("Build Scan worker 0"))
    assert(nextRegions.isEmpty)

    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      LinkIdentity(
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID)
          )
          .last
          .id,
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, hashJoin1.operatorID)
          )
          .head
          .id
      )
    )
    assert(nextRegions.isEmpty)
    nextRegions = scheduler.schedulingPolicy.onLinkCompletion(
      LinkIdentity(
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, buildCsv.operatorID)
          )
          .last
          .id,
        workflow.physicalPlan
          .layersOfLogicalOperator(
            new OperatorIdentity(workflow.workflowId.id, hashJoin2.operatorID)
          )
          .head
          .id
      )
    )
    assert(nextRegions.nonEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions().size == 1)
    scheduler.schedulingPolicy.addToRunningRegions(nextRegions)
    Set(probeCsv.operatorID, hashJoin1.operatorID, hashJoin2.operatorID, sink.operatorID).foreach(
      opID => {
        workflow.physicalPlan
          .layersOfLogicalOperator(new OperatorIdentity(workflow.workflowId.id, opID))
          .foreach(l => {
            l.workers.keys.foreach(wid => {
              l.workers(wid).state = COMPLETED
            })
          })
      }
    )
    nextRegions =
      scheduler.schedulingPolicy.onWorkerCompletion(ActorVirtualIdentity("Probe Scan worker 0"))
    assert(nextRegions.isEmpty)
    assert(scheduler.schedulingPolicy.getCompletedRegions().size == 2)
  }

}
