package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.twitter.util.{Await, Duration, Promise}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ExecutionStateUpdate,
  FatalError
}
import edu.uci.ics.amber.engine.architecture.controller._
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.model.WorkflowContext
import edu.uci.ics.amber.engine.common.model.WorkflowSettings
import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import edu.uci.ics.texera.workflow.common.workflow.{LogicalLink, WorkflowCompiler}
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.ExecutionStateStore

import scala.concurrent.duration.DurationInt

class BatchSizePropagationSpec
    extends TestKit(ActorSystem("BatchSizePropagationSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  val resultStorage = new OpResultStorage()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    resultStorage.close()
  }

  def executeWorkflow(workflow: Workflow): Unit = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      resultStorage,
      ControllerConfig.default,
      error => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => {
      completion.setException(evt.e)
      client.shutdown()
    })
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) {
        completion.setDone()
      }
    })
    Await.result(client.sendAsync(StartWorkflow()))
    Await.result(completion, Duration.fromMinutes(1))
  }

  "Engine" should "propagate the correct batch size into each WorkflowContext" in {
    val expectedBatchSize = 1

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()

    val workflowCompiler = new WorkflowCompiler(
      context
    )

    val workflow = workflowCompiler.compile(
      LogicalPlanPojo(
        List(headerlessCsvOpDesc, sink),
        List(
          LogicalLink(
            headerlessCsvOpDesc.operatorIdentifier,
            PortIdentity(),
            sink.operatorIdentifier,
            PortIdentity()
          )
        ),
        List(),
        List()
      ),
      resultStorage,
      new ExecutionStateStore()
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    var nextRegions = workflowScheduler.getNextRegions
    while (nextRegions.nonEmpty) {
      nextRegions.foreach { region =>
        region.resourceConfig.foreach { resourceConfig =>
          resourceConfig.linkConfigs.foreach {
            case (_, linkConfig) =>
              val partitioning = linkConfig.partitioning
              partitioning match {
                case oneToOne: OneToOnePartitioning =>
                  assert(
                    oneToOne.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${oneToOne.batchSize} != $expectedBatchSize"
                  )

                case roundRobin: RoundRobinPartitioning =>
                  assert(
                    roundRobin.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${roundRobin.batchSize} != $expectedBatchSize"
                  )

                case hashBased: HashBasedShufflePartitioning =>
                  assert(
                    hashBased.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${hashBased.batchSize} != $expectedBatchSize"
                  )

                case rangeBased: RangeBasedShufflePartitioning =>
                  assert(
                    rangeBased.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${rangeBased.batchSize} != $expectedBatchSize"
                  )

                case broadcast: BroadcastPartitioning =>
                  assert(
                    broadcast.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${broadcast.batchSize} != $expectedBatchSize"
                  )

                case _ =>
                  throw new IllegalArgumentException("Unknown partitioning type encountered")
              }
          }
        }
      }
      nextRegions = workflowScheduler.getNextRegions
    }
  }
}
