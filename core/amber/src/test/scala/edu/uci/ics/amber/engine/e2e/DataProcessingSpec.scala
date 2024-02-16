package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import ch.vorburger.mariadb4j.DB
import com.twitter.util.{Await, Duration, Promise}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller._
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType
import edu.uci.ics.texera.workflow.common.workflow._
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationFunction
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.PreparedStatement
import scala.concurrent.duration.DurationInt

class DataProcessingSpec
    extends TestKit(ActorSystem("DataProcessingSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  var inMemoryMySQLInstance: Option[DB] = None

  val resultStorage = new OpResultStorage()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    resultStorage.close()
  }

  def executeWorkflow(workflow: Workflow): Map[OperatorIdentity, List[ITuple]] = {
    var results: Map[OperatorIdentity, List[ITuple]] = null
    val client = new AmberClient(system, workflow, ControllerConfig.default, error => {})
    val completion = Promise[Unit]()
    client.registerCallback[FatalError](evt => {
      completion.setException(evt.e)
      client.shutdown()
    })

    client
      .registerCallback[WorkflowCompleted](evt => {
        results = workflow.logicalPlan.getTerminalOperatorIds
          .map(sinkOpId =>
            (sinkOpId, workflow.logicalPlan.getUpstreamOps(sinkOpId).head.operatorIdentifier)
          )
          .filter {
            case (_, upstreamOpId) => resultStorage.contains(upstreamOpId)
          }
          .map {
            case (sinkOpId, upstreamOpId) =>
              (sinkOpId, resultStorage.get(upstreamOpId).getAll.toList)
          }
          .toMap
        completion.setDone()
      })
    Await.result(client.sendAsync(StartWorkflow()))
    Await.result(completion, Duration.fromMinutes(1))
    results
  }

  def initializeInMemoryMySQLInstance(): (String, String, String, String, String, String) = {
    import ch.vorburger.mariadb4j.{DB, DBConfigurationBuilder}

    import java.sql.DriverManager

    val database: String = "new"
    val table: String = "test"
    val username: String = "root"
    val password: String = ""

    val config = DBConfigurationBuilder.newBuilder
      .setPort(0) // 0 => automatically detect free port
      .addArg("--default-time-zone=+0:00")
      .build()

    inMemoryMySQLInstance = Option(DB.newEmbeddedDB(config))
    inMemoryMySQLInstance.get.start()
    inMemoryMySQLInstance.get.createDB(database)

    // insert test data
    val conn = DriverManager.getConnection(config.getURL(database), username, password)
    var statement: PreparedStatement = conn.prepareStatement(
      s"create table $table (id int primary key auto_increment, text VARCHAR(512), " +
        s"point FLOAT, created_at DATE default NOW() not null)"
    )
    statement.execute()
    statement = conn.prepareStatement(s"insert into $table (text) values ('hello world')")
    statement.execute()
    statement.close()
    conn.close()
    ("localhost", config.getPort.toString, database, table, username, password)
  }

  "Engine" should "execute headerlessCsv->sink workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val results = executeWorkflow(workflow)(sink.operatorIdentifier)

    assert(results.size == 100)
  }

  "Engine" should "execute headerlessMultiLineDataCsv-->sink workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallMultiLineDataCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val results = executeWorkflow(workflow)(sink.operatorIdentifier)

    assert(results.size == 100)
  }

  "Engine" should "execute jsonl->sink workflow normally" in {
    val jsonlOp = TestOperators.smallJSONLScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(jsonlOp, sink),
      List(
        LogicalLink(
          jsonlOp.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val results = executeWorkflow(workflow)(sink.operatorIdentifier)

    assert(results.size == 100)

    for (result <- results) {
      val schema = result.asInstanceOf[Tuple].getSchema
      assert(schema.getAttribute("id").getType == AttributeType.LONG)
      assert(schema.getAttribute("first_name").getType == AttributeType.STRING)
      assert(schema.getAttribute("flagged").getType == AttributeType.BOOLEAN)
      assert(schema.getAttribute("year").getType == AttributeType.INTEGER)
      assert(schema.getAttribute("created_at").getType == AttributeType.TIMESTAMP)
      assert(schema.getAttributes.size() == 9)
    }

  }

  "Engine" should "execute mediumFlattenJsonl->sink workflow normally" in {
    val jsonlOp = TestOperators.mediumFlattenJSONLScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(jsonlOp, sink),
      List(
        LogicalLink(
          jsonlOp.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val results = executeWorkflow(workflow)(sink.operatorIdentifier)

    assert(results.size == 1000)

    for (result <- results) {
      val schema = result.asInstanceOf[Tuple].getSchema
      assert(schema.getAttribute("id").getType == AttributeType.LONG)
      assert(schema.getAttribute("first_name").getType == AttributeType.STRING)
      assert(schema.getAttribute("flagged").getType == AttributeType.BOOLEAN)
      assert(schema.getAttribute("year").getType == AttributeType.INTEGER)
      assert(schema.getAttribute("created_at").getType == AttributeType.TIMESTAMP)
      assert(schema.getAttribute("test_object.array2.another").getType == AttributeType.INTEGER)
      assert(schema.getAttributes.size() == 13)
    }
  }

  "Engine" should "execute headerlessCsv->keyword->sink workflow normally" in {
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
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc, sink),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, sink),
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
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->count->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, countOpDesc, sink),
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
        ),
        LogicalLink(
          countOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->averageAndGroupBy->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val averageAndGroupByOpDesc =
      TestOperators.aggregateAndGroupByDesc(
        "Units Sold",
        AggregationFunction.AVERAGE,
        List[String]("Country")
      )
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, averageAndGroupByOpDesc, sink),
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
        ),
        LogicalLink(
          averageAndGroupByOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->(csv->)->join->sink workflow normally" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc,
        sink
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
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)
  }

  // TODO: use mock data to perform the test, remove dependency on the real AsterixDB
  //  "Engine" should "execute asterixdb->sink workflow normally" in {
  //
  //    val asterixDBOp = TestOperators.asterixDBSourceOpDesc()
  //    val sink = TestOperators.sinkOpDesc()
  //    val (id, workflow) = buildWorkflow(
  //      List(asterixDBOp, sink),
  //      List(
  //        OperatorLink(OperatorPort(asterixDBOp.operatorIdentifier, 0), OperatorPort(sink.operatorIdentifier, 0))
  //      )
  //    )
  //    executeWorkflow(id, workflow)
  //  }

  "Engine" should "execute mysql->sink workflow normally" in {
    val (host, port, database, table, username, password) = initializeInMemoryMySQLInstance()
    val inMemoryMsSQLSourceOpDesc = TestOperators.inMemoryMySQLSourceOpDesc(
      host,
      port,
      database,
      table,
      username,
      password
    )

    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(inMemoryMsSQLSourceOpDesc, sink),
      List(
        LogicalLink(
          inMemoryMsSQLSourceOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    executeWorkflow(workflow)

    inMemoryMySQLInstance.get.stop()
  }

}
