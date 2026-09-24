/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.e2e

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.tuple.{AttributeType, Tuple}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.coordinator._
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadTerminalResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.concurrent.duration.DurationInt

class DataProcessingSpec
    extends TestKit(ActorSystem("DataProcessingSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  /**
    * This block retries each test once if it fails.
    * In the CI environment, there is a chance that executeWorkflow does not receive "COMPLETED" status.
    * Until we find the root cause of this issue, we use a retry mechanism here to stablize CI runs.
    */
  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  private val specId = 1

  val workflowContext: WorkflowContext = TestUtils.workflowContext(specId)

  val materializedWorkflowContext: WorkflowContext = TestUtils.workflowContext(
    specId,
    WorkflowSettings(
      dataTransferBatchSize = 400,
      executionMode = ExecutionMode.MATERIALIZED
    )
  )

  override protected def beforeEach(): Unit = {
    setUpWorkflowExecutionData(specId)
  }

  override protected def afterEach(): Unit = {
    cleanupWorkflowExecutionData(specId)
  }

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // These test cases access postgres in CI, but occasionally the jdbc driver cannot be found during CI run.
    // Explicitly load the JDBC driver to avoid flaky CI failures.
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def executeWorkflow(workflow: Workflow): Map[OperatorIdentity, List[Tuple]] =
    runWorkflowAndReadTerminalResults(system, workflow)

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

  /**
    * csv -> join(build), csv -> join(probe) -> Python UDF. The UDF shares a region with the join's
    * probe side, which has a dependee input port, so the UDF is only launched in the region's
    * second (non-dependee) phase.
    */
  private def joinThenPythonUdfWorkflow(udfCode: String): Workflow = {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val pythonOpDesc = TestOperators.pythonOpDesc()
    pythonOpDesc.code = udfCode
    buildWorkflow(
      List(headerlessCsvOpDesc1, headerlessCsvOpDesc2, joinOpDesc, pythonOpDesc),
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
          pythonOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )
  }

  private val tableUdfBody =
    """
      |class ProcessTableOperator(UDFTableOperator):
      |
      |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
      |        yield table
      |""".stripMargin

  "Engine" should "execute csv->(csv->)->join->python-udf workflow normally" in {
    executeWorkflow(joinThenPythonUdfWorkflow("from pytexera import *\n" + tableUdfBody))
  }

  "Engine" should "fail, not hang, when a python udf after a join fails to initialize" in {
    // Missing `from pytexera import *`, so `UDFTableOperator` is undefined when the worker
    // loads the code. Before the fix, the execution stayed RUNNING forever with every
    // operator of the region at 0 tuples.
    val error = intercept[Throwable] {
      executeWorkflow(joinThenPythonUdfWorkflow(tableUdfBody))
    }
    assert(error.getMessage.contains("UDFTableOperator"))
  }

  "Engine" should "execute headerlessCsv->keyword workflow with MATERIALIZED mode" in {
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
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv workflow with MATERIALIZED mode" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc),
      List(),
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword workflow with MATERIALIZED mode" in {
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
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->count workflow with MATERIALIZED mode" in {
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
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->keyword->averageAndGroupBy workflow with MATERIALIZED mode" in {
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
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }

  "Engine" should "execute csv->(csv->)->join workflow with MATERIALIZED mode" in {
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
      materializedWorkflowContext
    )
    executeWorkflow(workflow)
  }
}
