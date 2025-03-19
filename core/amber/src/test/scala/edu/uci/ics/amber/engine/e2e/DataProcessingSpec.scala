package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import ch.vorburger.mariadb4j.DB
import com.twitter.util.{Await, Duration, Promise}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.result.ExecutionResourcesMapping
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.{AttributeType, Tuple}
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PortIdentity, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller._
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.aggregate.AggregationFunction
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration.DurationInt

class DataProcessingSpec
    extends TestKit(ActorSystem("DataProcessingSpec", AmberRuntime.akkaConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  var inMemoryMySQLInstance: Option[DB] = None
  val workflowContext: WorkflowContext = new WorkflowContext()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def executeWorkflow(workflow: Workflow): Map[OperatorIdentity, List[Tuple]] = {
    var results: Map[OperatorIdentity, List[Tuple]] = null
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      error => {}
    )
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => {
      completion.setException(evt.e)
      client.shutdown()
    })

    client
      .registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          results = workflow.logicalPlan.getTerminalOperatorIds
            .filter(terminalOpId => {
              val uri = VFSURIFactory.createResultURI(
                workflowContext.workflowId,
                workflowContext.executionId,
                GlobalPortIdentity(
                  PhysicalOpIdentity(logicalOpId = terminalOpId, layerName = "main"),
                  PortIdentity()
                )
              )
              // expecting the first output port only.
              ExecutionResourcesMapping
                .getResourceURIs(workflowContext.executionId)
                .contains(uri)
            })
            .map(terminalOpId => {
              //TODO: remove the delay after fixing the issue of reporting "completed" status too early.
              Thread.sleep(1000)
              val uri = VFSURIFactory.createResultURI(
                workflowContext.workflowId,
                workflowContext.executionId,
                GlobalPortIdentity(
                  PhysicalOpIdentity(logicalOpId = terminalOpId, layerName = "main"),
                  PortIdentity()
                )
              )
              terminalOpId -> DocumentFactory
                .openDocument(uri)
                ._1
                .asInstanceOf[VirtualDocument[Tuple]]
                .get()
                .toList
            })
            .toMap
          completion.setDone()
        }
      })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    Await.result(completion, Duration.fromMinutes(1))
    results
  }

  "Engine" should "execute headerlessCsv workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc),
      List(),
      workflowContext
    )
    val results = executeWorkflow(workflow)(headerlessCsvOpDesc.operatorIdentifier)

    assert(results.size == 100)
  }

  "Engine" should "execute headerlessMultiLineDataCsv workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallMultiLineDataCsvScanOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc),
      List(),
      workflowContext
    )
    val results = executeWorkflow(workflow)(headerlessCsvOpDesc.operatorIdentifier)

    assert(results.size == 100)
  }

  "Engine" should "execute jsonl workflow normally" in {
    val jsonlOp = TestOperators.smallJSONLScanOpDesc()
    val workflow = buildWorkflow(
      List(jsonlOp),
      List(),
      workflowContext
    )
    val results = executeWorkflow(workflow)(jsonlOp.operatorIdentifier)

    assert(results.size == 100)

    for (result <- results) {
      val schema = result.getSchema
      assert(schema.getAttribute("id").getType == AttributeType.LONG)
      assert(schema.getAttribute("first_name").getType == AttributeType.STRING)
      assert(schema.getAttribute("flagged").getType == AttributeType.BOOLEAN)
      assert(schema.getAttribute("year").getType == AttributeType.INTEGER)
      assert(schema.getAttribute("created_at").getType == AttributeType.TIMESTAMP)
      assert(schema.getAttributes.length == 9)
    }

  }

  "Engine" should "execute mediumFlattenJsonl workflow normally" in {
    val jsonlOp = TestOperators.mediumFlattenJSONLScanOpDesc()
    val workflow = buildWorkflow(
      List(jsonlOp),
      List(),
      workflowContext
    )
    val results = executeWorkflow(workflow)(jsonlOp.operatorIdentifier)

    assert(results.size == 1000)

    for (result <- results) {
      val schema = result.getSchema
      assert(schema.getAttribute("id").getType == AttributeType.LONG)
      assert(schema.getAttribute("first_name").getType == AttributeType.STRING)
      assert(schema.getAttribute("flagged").getType == AttributeType.BOOLEAN)
      assert(schema.getAttribute("year").getType == AttributeType.INTEGER)
      assert(schema.getAttribute("created_at").getType == AttributeType.TIMESTAMP)
      assert(schema.getAttribute("test_object.array2.another").getType == AttributeType.INTEGER)
      assert(schema.getAttributes.length == 13)
    }
  }

  "Engine" should "execute headerlessCsv->keyword workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc),
      List(),
      workflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->count workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, countOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          countOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->averageAndGroupBy workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val averageAndGroupByOpDesc =
      TestOperators.aggregateAndGroupByDesc(
        "Units Sold",
        AggregationFunction.AVERAGE,
        List[String]("Country")
      )
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, averageAndGroupByOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          averageAndGroupByOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->(csv->)->join workflow normally" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc2.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      workflowContext
    )
    executeWorkflow(workflow)
  }
}
