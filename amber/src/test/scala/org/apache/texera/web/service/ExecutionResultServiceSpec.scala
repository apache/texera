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

package org.apache.texera.web.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.storage.result.{OperatorResultMetadata, WorkflowResultStore}
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  OutputPort,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  FatalError,
  OperatorPortResultUriAvailable
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  OPERATOR_PORT_EXECUTIONS,
  USER,
  WORKFLOW,
  WORKFLOW_COMPUTING_UNIT,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent
}
import org.apache.texera.web.model.websocket.request.ResultPaginationRequest
import org.apache.texera.web.service.ExecutionResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebDataUpdate,
  WebPaginationUpdate
}
import org.apache.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.sql.Timestamp
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class ExecutionResultServiceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Fixed (not random) so a failure replays identically across runs. The
  // spec owns its embedded DB so collision with other specs isn't a concern.
  private val testWid: Integer = 9001
  private val testUid: Integer = 9001
  private var executionsDao: WorkflowExecutionsDao = _
  private var testVid: Integer = _
  private var testCuid: Integer = _

  // AmberClient needs an ActorSystem to host its ClientActor. A bare one is enough:
  // the client is constructed over an empty PhysicalPlan, so its InitializeRequest
  // completes without an engine, and the subclass below never sends anything to it.
  private var system: ActorSystem = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    system = ActorSystem("ExecutionResultServiceSpec")
  }

  override protected def afterAll(): Unit = {
    system.terminate()
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    val user = new User
    user.setUid(testUid)
    user.setName("execution-result-test-user")
    user.setEmail(s"u$testUid@example.com")
    new UserDao(getDSLContext.configuration()).insert(user)

    // getLatestExecutionId matches on cuid, and a NULL cuid matches no value, so
    // handleResultPagination can only find an execution that names a real unit.
    val computingUnit = new WorkflowComputingUnit
    computingUnit.setUid(testUid)
    computingUnit.setName("execution-result-test-unit")
    computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))
    computingUnit.setType(WorkflowComputingUnitTypeEnum.local)
    computingUnit.setUri("local://execution-result-test")
    computingUnit.setResource("{}")
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(computingUnit)
    testCuid = computingUnit.getCuid

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"execution-result-test-$testWid")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)
    // The vid sequence isn't reset between tests, so capture the
    // generated key here instead of assuming it's `1` later.
    testVid = version.getVid

    executionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
  }

  override protected def afterEach(): Unit = {
    val ctx = getDSLContext
    // Scope every delete to the test's own ids so this spec stays safe
    // if it ever shares a DB with another spec.
    ctx
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          ctx
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
        )
      )
      .execute()
    ctx
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
      .execute()
    ctx.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()
    ctx.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    ctx.deleteFrom(WORKFLOW_COMPUTING_UNIT).where(WORKFLOW_COMPUTING_UNIT.UID.eq(testUid)).execute()
    ctx.deleteFrom(USER).where(USER.UID.eq(testUid)).execute()
  }

  "persistOperatorPortResultUri" should
    "insert the URI carried by an OperatorPortResultUriAvailable event" in {
    val execution = new WorkflowExecutions
    execution.setVid(testVid)
    execution.setUid(testUid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("execution-result-callback-test")
    execution.setEnvironmentVersion("test-env")
    executionsDao.insert(execution)
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op-X"), "main"),
      PortIdentity(),
      input = false
    )
    val uri = URI.create("vfs:///exec-result-callback")

    ExecutionResultService.persistOperatorPortResultUri(
      eid,
      OperatorPortResultUriAvailable(globalPortId, uri)
    )

    val rows = getDSLContext
      .selectFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .fetch()
    rows.size() shouldBe 1
    rows.get(0).getResultUri shouldBe uri.toString
  }

  "convertTuplesToJson" should "convert tuples with various field types correctly" in {
    // Create a schema with different attribute types
    val attributes = List(
      new Attribute("stringCol", AttributeType.STRING),
      new Attribute("intCol", AttributeType.INTEGER),
      new Attribute("boolCol", AttributeType.BOOLEAN),
      new Attribute("nullCol", AttributeType.ANY),
      new Attribute("longStringCol", AttributeType.STRING),
      new Attribute("shortBinaryCol", AttributeType.BINARY),
      new Attribute("longBinaryCol", AttributeType.BINARY)
    )

    val schema = new Schema(attributes)

    // Create a string longer than maxStringLength (100)
    val longString = "a" * 150

    // Create binary data
    val shortBinaryData = Array[Byte](1, 2, 3, 4, 5)
    val longBinaryData = Array.tabulate[Byte](100)(_.toByte)

    // Create a tuple with all the test data
    val tuple = Tuple
      .builder(schema)
      .add("stringCol", AttributeType.STRING, "regular string")
      .add("intCol", AttributeType.INTEGER, 42)
      .add("boolCol", AttributeType.BOOLEAN, true)
      .add("nullCol", AttributeType.ANY, null)
      .add("longStringCol", AttributeType.STRING, longString)
      .add("shortBinaryCol", AttributeType.BINARY, shortBinaryData)
      .add("longBinaryCol", AttributeType.BINARY, longBinaryData)
      .build()

    // Convert to JSON
    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    // Verify the result
    result should have size 1
    val jsonNode = result.head

    // Check regular values
    jsonNode.get("stringCol").asText() shouldBe "regular string"
    jsonNode.get("intCol").asInt() shouldBe 42
    jsonNode.get("boolCol").asBoolean() shouldBe true

    // Check NULL value
    jsonNode.get("nullCol").asText() shouldBe "NULL"

    // Check long string truncation
    jsonNode.get("longStringCol").asText() should (
      have length 103 and // 100 chars + "..."
        startWith("a" * 100) and
        endWith("...")
    )

    // Check short binary representation
    val shortBinaryString = jsonNode.get("shortBinaryCol").asText()
    shortBinaryString should (
      startWith("<binary") and
        include("...") and
        include("size = 5 bytes")
    )

    // Check long binary representation
    val longBinaryString = jsonNode.get("longBinaryCol").asText()
    longBinaryString should (
      startWith("<binary") and
        include("...") and
        include("size = 100 bytes")
    )
  }

  it should "handle empty collections of tuples" in {
    val result = ExecutionResultService.convertTuplesToJson(List())
    result shouldBe empty
  }

  it should "handle collections with multiple tuples" in {
    // Create a simple schema
    val attributes = List(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val schema = new Schema(attributes)

    // Create multiple tuples
    val tuple1 = Tuple
      .builder(schema)
      .add("id", AttributeType.INTEGER, 1)
      .add("name", AttributeType.STRING, "Alice")
      .build()

    val tuple2 = Tuple
      .builder(schema)
      .add("id", AttributeType.INTEGER, 2)
      .add("name", AttributeType.STRING, "Bob")
      .build()

    // Convert to JSON
    val results = ExecutionResultService.convertTuplesToJson(List(tuple1, tuple2))

    // Verify the results
    results should have size 2
    results.head.get("id").asInt() shouldBe 1
    results.head.get("name").asText() shouldBe "Alice"
    results(1).get("id").asInt() shouldBe 2
    results(1).get("name").asText() shouldBe "Bob"
  }

  it should "handle string exactly at the maximum length" in {
    val attributes = List(
      new Attribute("exactLengthString", AttributeType.STRING)
    )
    val schema = new Schema(attributes)

    // Create string exactly at maxStringLength (100)
    val exactLengthString = "x" * 100

    val tuple = Tuple
      .builder(schema)
      .add("exactLengthString", AttributeType.STRING, exactLengthString)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    jsonNode.get("exactLengthString").asText() shouldBe exactLengthString
    jsonNode.get("exactLengthString").asText() should have length 100
  }

  it should "handle empty binary data" in {
    val attributes = List(
      new Attribute("emptyBinary", AttributeType.BINARY)
    )
    val schema = new Schema(attributes)

    // Empty binary data
    val emptyBinaryData = Array[Byte]()

    val tuple = Tuple
      .builder(schema)
      .add("emptyBinary", AttributeType.BINARY, emptyBinaryData)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    val emptyBinaryString = jsonNode.get("emptyBinary").asText()
    emptyBinaryString should include("size = 0 bytes")
  }

  it should "handle binary data with single ByteBuffer" in {
    val attributes = List(
      new Attribute("singleBufferBinary", AttributeType.BINARY)
    )
    val schema = new Schema(attributes)

    // Create binary data with a single ByteBuffer
    val singleBufferData = "Hello, world!".getBytes()

    val tuple = Tuple
      .builder(schema)
      .add("singleBufferBinary", AttributeType.BINARY, singleBufferData)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    val binaryString = jsonNode.get("singleBufferBinary").asText()
    binaryString should (
      startWith("<binary") and
        include("size = 13 bytes") // "Hello, world!" is 13 bytes
    )
  }

  it should "handle various numeric types correctly" in {
    val attributes = List(
      new Attribute("intValue", AttributeType.INTEGER),
      new Attribute("doubleValue", AttributeType.DOUBLE),
      new Attribute("longValue", AttributeType.LONG)
    )
    val schema = new Schema(attributes)

    val tuple = Tuple
      .builder(schema)
      .add("intValue", AttributeType.INTEGER, Int.MaxValue)
      .add("doubleValue", AttributeType.DOUBLE, 3.14159)
      .add("longValue", AttributeType.LONG, Long.MaxValue)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    jsonNode.get("intValue").asInt() shouldBe Int.MaxValue
    jsonNode.get("doubleValue").asDouble() shouldBe 3.14159
    jsonNode.get("longValue").asLong() shouldBe Long.MaxValue
  }

  it should "handle multiple binary fields within the same tuple" in {
    val attributes = List(
      new Attribute("binaryField1", AttributeType.BINARY),
      new Attribute("binaryField2", AttributeType.BINARY)
    )
    val schema = new Schema(attributes)

    val binaryData1 = Array[Byte](10, 20, 30)
    val binaryData2 = Array[Byte](40, 50, 60)

    val tuple = Tuple
      .builder(schema)
      .add("binaryField1", AttributeType.BINARY, binaryData1)
      .add("binaryField2", AttributeType.BINARY, binaryData2)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    val binaryString1 = jsonNode.get("binaryField1").asText()
    binaryString1 should (
      startWith("<binary") and
        include("size = 3 bytes")
    )

    val binaryString2 = jsonNode.get("binaryField2").asText()
    binaryString2 should (
      startWith("<binary") and
        include("size = 3 bytes")
    )
  }

  it should "not truncate long strings when isVisualization is true" in {
    val attributes = List(
      new Attribute("longStringCol", AttributeType.STRING)
    )
    val schema = new Schema(attributes)

    // Create a string longer than maxStringLength (100)
    val longString = "a" * 150
    val htmlVisualizationString = """
      <head>
        <meta charset="utf-8" />
      </head>
      <body>
        <div>
          <script type="text/javascript">
            window.PlotlyConfig = {MathJaxConfig: 'local'};
          </script>
          <script charset="utf-8" src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
          <div id="740a52d7-d771-417c-a197-28a29a048f95" class="plotly-graph-div" style="height:100%; width:100%;"></div>
          <script type="text/javascript">
            window.PLOTLYENV=window.PLOTLYENV || {};
            if (document.getElementById("740a52d7-d771-417c-a197-28a29a048f95")) {
              Plotly.newPlot(
                "740a52d7-d771-417c-a197-28a29a048f95",
                [
                  {
                    "alignmentgroup": "True",
                    "hovertemplate": "Item Type=%{x}<br>units-sold-per-type=%{y}<extra></extra>",
                    "legendgroup": "",
                    "marker": {"color": "#636efa", "pattern": {"shape": ""}},
                    "name": "",
                    "offsetgroup": "",
                    "orientation": "v",
                    "showlegend": false,
                    "textposition": "auto",
                    "x": [
                      "Vegetables", 
                      "Office Supplies", 
                      "Baby Food", 
                      "Household", 
                      "Cosmetics", 
                      "Beverages", 
                      "Personal Care", 
                      "Clothes"
                    ],
                    "xaxis": "x",
                    "y": [171.0, 3958.0, 6552.5, 2397.5, 6414.75, 4892.0, 2671.5, 3513.25],
                    "yaxis": "y",
                    "type": "bar"
                  }
                ],
                {
                  "barmode": "relative",
                  "legend": {"tracegroupgap": 0},
                  "margin": {"t": 0, "l": 0, "r": 0, "b": 0},
                  "template": {
                    "data": {
                      "barpolar": [
                        {
                          "marker": {
                            "line": {"color": "#E5ECF6", "width": 0.5},
                            "pattern": {"fillmode": "overlay", "size": 10, "solidity": 0.2}
                          },
                          "type": "barpolar"
                        }
                      ],
                      "bar": [
                        {
                          "error_x": {"color": "#2a3f5f"},
                          "error_y": {"color": "#2a3f5f"},
                          "marker": {
                            "line": {"color": "#E5ECF6", "width": 0.5},
                            "pattern": {"fillmode": "overlay", "size": 10, "solidity": 0.2}
                          },
                          "type": "bar"
                        }
                      ],
                      // Additional template data omitted for brevity
                    },
                    "layout": {
                      // Layout configuration omitted for brevity
                    }
                  },
                  "xaxis": {"anchor": "y", "domain": [0.0, 1.0], "title": {"text": "Item Type"}},
                  "yaxis": {"anchor": "x", "domain": [0.0, 1.0], "title": {"text": "units-sold-per-type"}}
                },
                {"responsive": true}
              )
            };
          </script>
        </div>
      </body>
    </html>"""

    // Test case 1: With a simple long string
    val tuple1 = Tuple
      .builder(schema)
      .add("longStringCol", AttributeType.STRING, longString)
      .build()

    // Test case 2: With HTML visualization content
    val tuple2 = Tuple
      .builder(schema)
      .add("longStringCol", AttributeType.STRING, htmlVisualizationString)
      .build()

    // When isVisualization is false (default)
    val resultsDefault = ExecutionResultService.convertTuplesToJson(List(tuple1, tuple2))

    // Verify truncation happens
    resultsDefault(0).get("longStringCol").asText() should (
      have length 103 and // 100 chars + "..."
        startWith("a" * 100) and
        endWith("...")
    )

    resultsDefault(1).get("longStringCol").asText() should (
      have length 103 and
        endWith("...")
    )

    // When isVisualization is true
    val resultsVisualization =
      ExecutionResultService.convertTuplesToJson(List(tuple1, tuple2), true)

    // Verify no truncation happens
    resultsVisualization(0).get("longStringCol").asText() shouldBe longString
    resultsVisualization(0).get("longStringCol").asText() should have length 150

    resultsVisualization(1).get("longStringCol").asText() shouldBe htmlVisualizationString
    resultsVisualization(1)
      .get("longStringCol")
      .asText() should have length htmlVisualizationString.length
  }

  it should "handle direct comparison between non-visualization and visualization mode" in {
    val attributes = List(
      new Attribute("col1", AttributeType.STRING),
      new Attribute("col2", AttributeType.STRING),
      new Attribute("col3", AttributeType.STRING)
    )
    val schema = new Schema(attributes)

    // Create strings of various lengths
    val shortString = "short string" // under maxStringLength
    val exactLengthString = "x" * 100 // exactly maxStringLength
    val longString = "y" * 200 // over maxStringLength

    val tuple = Tuple
      .builder(schema)
      .add("col1", AttributeType.STRING, shortString)
      .add("col2", AttributeType.STRING, exactLengthString)
      .add("col3", AttributeType.STRING, longString)
      .build()

    // Convert with both modes
    val resultDefault = ExecutionResultService.convertTuplesToJson(List(tuple), false)
    val resultVisualization = ExecutionResultService.convertTuplesToJson(List(tuple), true)

    // Short strings should be the same in both modes
    resultDefault(0).get("col1").asText() shouldBe shortString
    resultVisualization(0).get("col1").asText() shouldBe shortString

    // Exact length strings should be the same in both modes
    resultDefault(0).get("col2").asText() shouldBe exactLengthString
    resultVisualization(0).get("col2").asText() shouldBe exactLengthString

    // Long strings should be truncated in default mode but not in visualization mode
    resultDefault(0).get("col3").asText() should (
      have length 103 and // 100 chars + "..."
        startWith("y" * 100) and
        endWith("...")
    )
    resultVisualization(0).get("col3").asText() shouldBe longString
    resultVisualization(0).get("col3").asText() should have length 200
  }

  it should "apply visualization flag correctly to mixed collections" in {
    val attributes = List(
      new Attribute("value", AttributeType.STRING)
    )
    val schema = new Schema(attributes)

    // Create a collection with both short and long strings
    val tuples = List(
      Tuple.builder(schema).add("value", AttributeType.STRING, "short").build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "a" * 150).build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "medium length").build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "b" * 200).build()
    )

    // Test with visualization flag true
    val resultsVisualization = ExecutionResultService.convertTuplesToJson(tuples, true)

    // All strings should remain intact
    resultsVisualization(0).get("value").asText() shouldBe "short"
    resultsVisualization(1).get("value").asText() shouldBe "a" * 150
    resultsVisualization(2).get("value").asText() shouldBe "medium length"
    resultsVisualization(3).get("value").asText() shouldBe "b" * 200

    // Test with visualization flag false (default)
    val resultsDefault = ExecutionResultService.convertTuplesToJson(tuples)

    // Short strings unchanged, long strings truncated
    resultsDefault(0).get("value").asText() shouldBe "short"
    resultsDefault(1).get("value").asText() should endWith("...")
    resultsDefault(2).get("value").asText() shouldBe "medium length"
    resultsDefault(3).get("value").asText() should endWith("...")
  }

  // The existing binary cases assert only the byte size and the presence of an
  // ellipsis. They never pin the actual preview bits, and they never exercise
  // the "<= 13 bits" branch with a non-empty array (only the empty-array edge
  // hits it). The next two cases pin both branches exactly.

  it should "render the full bit string for a binary field of 13 bits or fewer" in {
    val schema = new Schema(List(new Attribute("b", AttributeType.BINARY)))
    // 1 byte = 8 bits <= 10 (leading) + 3 (trailing), so no truncation/ellipsis.
    val tuple = Tuple.builder(schema).add("b", AttributeType.BINARY, Array[Byte](5)).build()

    val text = ExecutionResultService.convertTuplesToJson(List(tuple)).head.get("b").asText()
    text shouldBe "<binary 00000101, size = 1 bytes>"
  }

  it should "render leading-10 and trailing-3 preview bits with an ellipsis for larger binary fields" in {
    val schema = new Schema(List(new Attribute("b", AttributeType.BINARY)))
    // 3 bytes = 24 bits > 13: preview = first 10 bits + "..." + last 3 bits.
    // bytes 0xFF,0x00,0xAA -> "11111111 00000000 10101010"
    //   leading 10 bits  = "1111111100"
    //   trailing 3 bits  = "010"
    val bytes = Array[Byte](0xff.toByte, 0x00.toByte, 0xaa.toByte)
    val tuple = Tuple.builder(schema).add("b", AttributeType.BINARY, bytes).build()

    val text = ExecutionResultService.convertTuplesToJson(List(tuple)).head.get("b").asText()
    text shouldBe "<binary 1111111100...010, size = 3 bytes>"
  }

  // TIMESTAMP passes through to the shared objectMapper unchanged.
  it should "pass timestamp fields through unchanged to the shared serializer" in {
    val schema = new Schema(List(new Attribute("ts", AttributeType.TIMESTAMP)))
    val ts = Timestamp.valueOf("2023-01-15 08:30:45.123")
    val tuple = Tuple.builder(schema).add("ts", AttributeType.TIMESTAMP, ts).build()

    val node = ExecutionResultService.convertTuplesToJson(List(tuple)).head.get("ts")
    node shouldBe objectMapper.valueToTree[JsonNode](ts)
  }

  // The WebOutputMode / WebResultUpdate ADTs are serialized to the frontend over
  // the websocket; the "type" discriminator is the contract the UI dispatches on.

  "WebOutputMode serialization" should "tag each mode with its discriminator" in {
    objectMapper
      .valueToTree[ObjectNode](PaginationMode())
      .get("type")
      .asText() shouldBe "PaginationMode"
    objectMapper
      .valueToTree[ObjectNode](SetSnapshotMode())
      .get("type")
      .asText() shouldBe "SetSnapshotMode"
    objectMapper
      .valueToTree[ObjectNode](SetDeltaMode())
      .get("type")
      .asText() shouldBe "SetDeltaMode"
  }

  "WebPaginationUpdate serialization" should "carry the total count, dirty pages, and mode discriminator" in {
    val json =
      objectMapper.valueToTree[ObjectNode](WebPaginationUpdate(PaginationMode(), 7L, List(1, 3)))
    json.get("mode").get("type").asText() shouldBe "PaginationMode"
    json.get("totalNumTuples").asLong() shouldBe 7L
    json.get("dirtyPageIndices").elements().asScala.map(_.asInt()).toList shouldBe List(1, 3)
  }

  "WebDataUpdate serialization" should "carry the mode discriminator and the table rows" in {
    val row = objectMapper.createObjectNode()
    row.put("k", "v")
    val json = objectMapper.valueToTree[ObjectNode](WebDataUpdate(SetSnapshotMode(), List(row)))
    json.get("mode").get("type").asText() shouldBe "SetSnapshotMode"
    val table = json.get("table")
    table.size() shouldBe 1
    table.get(0).get("k").asText() shouldBe "v"
  }

  // ==========================================================================
  // The service instance: handleResultPagination, and the callbacks / result
  // diff handler that attachToExecution wires up.
  //
  // Two seams make this reachable without an engine:
  //   * `client` is a parameter of attachToExecution and `registerCallback` is
  //     overridable, so `RecordingAmberClient` captures the callbacks the
  //     service registers and each test fires one directly.
  //   * the result-polling `Cancellable` is created by AmberRuntime's scheduler
  //     once the execution reaches RUNNING. AmberRuntime holds its ActorSystem
  //     in a private var that only a started pekko cluster fills in, so the
  //     service's own private field is set directly (`setPollingCancellable`)
  //     to reproduce the post-RUNNING state that the terminal-state, fatal-error
  //     and re-attach paths all assume. Nothing global is mutated, and no
  //     RUNNING state is ever pushed through the metadata store.
  //
  // The result documents are real Iceberg tables. Their storage key is derived
  // from the URI path, and creating one overwrites whatever is already there, so
  // every logical operator id below carries an `erss-` prefix that is unique in
  // the repository -- sbt runs amber suites in parallel inside one JVM.
  // ==========================================================================

  private val paginationRequest =
    ResultPaginationRequest(requestID = "req-1", operatorID = "", pageIndex = 1, pageSize = 3)

  "handleResultPagination" should "return the rows of the requested page" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("label", AttributeType.STRING))
    )
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i =>
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, i)
          .add("label", AttributeType.STRING, s"row-$i")
          .build()
      )
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 2, pageSize = 3)
    )

    // Page 2 of size 3 is the half-open range [3, 6): both the `pageSize *
    // (pageIndex - 1)` origin and the `from + pageSize` bound are pinned by the
    // exact id list, which shifts under an off-by-one on either end.
    event.table.map(_.get("id").asInt()) shouldBe List(3, 4, 5)
    event.table.map(_.get("label").asText()) shouldBe List("row-3", "row-4", "row-5")
    // The request fields are echoed back so the frontend can match the reply to
    // the outstanding page request.
    event.requestID shouldBe "req-1"
    event.operatorID shouldBe "erss-page-rows"
    event.pageIndex shouldBe 2
    event.schema.map(_.getName) shouldBe List("id", "label")
  }

  it should "clamp the last page to the rows that exist" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 3, pageSize = 3)
    )

    event.table.map(_.get("id").asInt()) shouldBe List(6)
  }

  it should "return an empty page with no schema past the end of the result" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 4, pageSize = 3)
    )

    event.table shouldBe empty
    // The reported schema comes from the first row of the page, so a page with no
    // rows reports no schema at all -- there is no fallback to the stored schema.
    event.schema shouldBe empty
  }

  it should "match a column search case-insensitively" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(
        new Attribute("userName", AttributeType.STRING),
        new Attribute("UserAge", AttributeType.STRING),
        new Attribute("city", AttributeType.STRING)
      )
    )
    storeResult(
      executionId,
      "erss-col-search",
      schema,
      List(
        Tuple
          .builder(schema)
          .add("userName", AttributeType.STRING, "ada")
          .add("UserAge", AttributeType.STRING, "36")
          .add("city", AttributeType.STRING, "london")
          .build()
      )
    )

    // "eRNa" matches "userName" only when BOTH sides are lower-cased: dropping
    // the column's `.toLowerCase` leaves "userName".contains("erna") false, and
    // dropping the search term's leaves "username".contains("eRNa") false.
    val event = paginate(
      paginationRequest
        .copy(operatorID = "erss-col-search", columnSearch = Some("eRNa"))
    )

    event.table.map(_.fieldNames().asScala.toList) shouldBe List(List("userName"))
    event.table.head.get("userName").asText() shouldBe "ada"
    event.schema.map(_.getName) shouldBe List("userName")
  }

  it should "slice the projected columns by offset and limit" in {
    val executionId = newExecution()
    val columnNames = List("c0", "c1", "c2", "c3")
    val schema = new Schema(columnNames.map(new Attribute(_, AttributeType.STRING)))
    storeResult(
      executionId,
      "erss-col-slice",
      schema,
      List(
        columnNames
          .foldLeft(Tuple.builder(schema))((b, c) => b.add(c, AttributeType.STRING, s"v-$c"))
          .build()
      )
    )

    // slice's second argument is an end index, not a count: `slice(offset, limit)`
    // would return only "c1" here.
    val event = paginate(
      paginationRequest.copy(operatorID = "erss-col-slice", columnOffset = 1, columnLimit = 2)
    )

    event.table.map(_.fieldNames().asScala.toList) shouldBe List(List("c1", "c2"))
    event.schema.map(_.getName) shouldBe List("c1", "c2")
  }

  it should "return an empty page when the operator has no stored result" in {
    newExecution()

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-never-stored", pageIndex = 2)
    )

    // The reply still has to identify the request it answers, otherwise the
    // frontend cannot retire the pending page.
    event.requestID shouldBe "req-1"
    event.operatorID shouldBe "erss-never-stored"
    event.pageIndex shouldBe 2
    event.table shouldBe empty
    event.schema shouldBe empty
  }

  it should "fail loudly when the workflow has never been executed" in {
    // No execution row at all: distinct from the case above, where an execution
    // exists but stored no result for the operator.
    val thrown = the[IllegalStateException] thrownBy paginate(
      paginationRequest.copy(operatorID = "erss-page-rows")
    )
    thrown.getMessage shouldBe "No execution is recorded"
  }

  it should "refuse to read a result stored in a per-user warehouse while the feature is off" in {
    val executionId = newExecution()
    // Only the URI row is needed: the guard has to reject before the document is
    // opened, and no table was ever created for this warehouse. The guard reads
    // StorageConfig.warehouseEnabled, which ships (and runs in CI) as false;
    // WarehouseReadGuardSpec pins both settings of the flag directly.
    insertResultUri(executionId, resultUriOf(executionId, "erss-warehouse", Some("byo")))

    a[WarehouseUnavailableException] should be thrownBy paginate(
      paginationRequest.copy(operatorID = "erss-warehouse")
    )
  }

  // -- attachToExecution: the result-store diff handler -----------------------

  "the result diff handler" should "send a pagination update for a SET_SNAPSHOT output port" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-snapshot",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    // The internal port is listed FIRST so that dropping the `!portId.internal`
    // filter would read SET_DELTA's mode instead of the external port's.
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-snapshot",
        PortIdentity(1, internal = true) -> OutputMode.SET_DELTA,
        PortIdentity() -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-snapshot" -> 7)

      // 7 rows at the default page size of 5 dirty two pages; the count sent to
      // the frontend is the new count, not the old one.
      events.updates.map(_.updates) shouldBe List(
        Map("erss-snapshot" -> WebPaginationUpdate(PaginationMode(), 7L, List(1, 2)))
      )
    }
  }

  it should "record the size and per-column statistics of a table-mode result" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("name", AttributeType.STRING))
    )
    val uri = storeResult(
      executionId,
      "erss-snapshot-stats",
      schema,
      List(
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, 1)
          .add("name", AttributeType.STRING, "ada")
          .build()
      )
    )
    val plan = planOf(
      physicalOp(executionId, "erss-snapshot-stats", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-snapshot-stats" -> 1)

      events.updates.head.tableStats.keySet shouldBe Set("erss-snapshot-stats")
      events.updates.head.tableStats("erss-snapshot-stats").keySet shouldBe Set("id", "name")

      // The persisted size must be the document's own file size, not some other
      // measure of the result (a row count, or a hard-coded zero).
      val expectedSize = DocumentFactory.openDocument(uri)._1.getTotalFileSize
      expectedSize should not be 0L
      storedResultSize(executionId, "erss-snapshot-stats") shouldBe expectedSize
    }
  }

  it should "send the whole untruncated snapshot, and no statistics, for a SINGLE_SNAPSHOT port" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    val html = "a" * 150
    // Two rows, so that reading only the head of the document would be visible.
    storeResult(
      executionId,
      "erss-single",
      schema,
      List(html, "<p>second</p>")
        .map(Tuple.builder(schema).add("html", AttributeType.STRING, _).build())
    )
    val plan =
      planOf(physicalOp(executionId, "erss-single", PortIdentity() -> OutputMode.SINGLE_SNAPSHOT))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-single" -> 2)

      val update = dataUpdate(events.updates.head.updates("erss-single"))
      update.mode shouldBe SetSnapshotMode()
      // SINGLE_SNAPSHOT carries rendered HTML, so the 100-character string
      // truncation must be switched off for it.
      update.table.map(_.get("html").asText()) shouldBe List(html, "<p>second</p>")
      // A SINGLE_SNAPSHOT port's document is not a table, so no statistics are
      // gathered and no size is recorded for it -- while the data update itself
      // is still sent.
      events.updates.head.tableStats shouldBe empty
      storedResultSize(executionId, "erss-single") shouldBe 0L
    }
  }

  it should "send only the rows added since the previous update for a SET_DELTA port" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("note", AttributeType.STRING))
    )
    val longNote = "b" * 150
    storeResult(
      executionId,
      "erss-delta",
      schema,
      (0 until 5).map(i =>
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, i)
          .add("note", AttributeType.STRING, longNote)
          .build()
      )
    )
    val plan = planOf(physicalOp(executionId, "erss-delta", PortIdentity() -> OutputMode.SET_DELTA))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)

      fixture.setResultCounts("erss-delta" -> 2)
      fixture.setResultCounts("erss-delta" -> 5)

      val tables =
        events.updates.map(e => dataUpdate(e.updates("erss-delta")).table.map(_.get("id").asInt()))
      // The delta is read from the PREVIOUS count, so the second update repeats
      // nothing: reading from the new count would send an empty table, and
      // reading from zero would resend rows 0 and 1.
      tables shouldBe List(List(0, 1, 2, 3, 4), List(2, 3, 4))

      val deltaUpdate = dataUpdate(events.updates.head.updates("erss-delta"))
      deltaUpdate.mode shouldBe SetDeltaMode()
      // Unlike SINGLE_SNAPSHOT, a delta is table data, so long strings are
      // truncated for display.
      deltaUpdate.table.head.get("note").asText() shouldBe ("b" * 100 + "...")
    }
  }

  it should "send an empty pagination update when the result is not stored yet" in {
    val executionId = newExecution()
    val plan =
      planOf(physicalOp(executionId, "erss-unstored", PortIdentity() -> OutputMode.SET_SNAPSHOT))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-unstored" -> 4)

      // Storage only exists once the operator's region has been scheduled. Until
      // then the reported count is zero, not the count the engine reported.
      events.updates.map(_.updates) shouldBe List(
        Map("erss-unstored" -> WebPaginationUpdate(PaginationMode(), 0L, List.empty))
      )
    }
  }

  it should "reject an output mode it does not recognize" in {
    val executionId = newExecution()
    val plan = planOf(
      physicalOp(executionId, "erss-bad-mode", PortIdentity() -> OutputMode.Unrecognized(99))
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-bad-mode" -> 1)

      events.updates shouldBe empty
      // The unrecognized enum value renders as "UNRECOGNIZED", so the workflow id
      // is the only thing in the message that identifies what failed.
      events.errors.map(_.getMessage) shouldBe List(
        s"Unrecognized output mode: UNRECOGNIZED for workflow $testWid"
      )
    }
  }

  it should "update only the operators whose tuple count changed" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val rows = List(Tuple.builder(schema).add("id", AttributeType.INTEGER, 1).build())
    storeResult(executionId, "erss-changed-a", schema, rows)
    storeResult(executionId, "erss-changed-b", schema, rows)
    val plan = planOf(
      physicalOp(executionId, "erss-changed-a", PortIdentity() -> OutputMode.SET_SNAPSHOT),
      physicalOp(executionId, "erss-changed-b", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-changed-a" -> 1, "erss-changed-b" -> 1)
      fixture.setResultCounts("erss-changed-a" -> 1, "erss-changed-b" -> 2)

      // Recomputing an unchanged operator would re-read its whole document on
      // every poll, so the second diff must mention only the operator that moved.
      events.updates.map(_.updates.keySet) shouldBe List(
        Set("erss-changed-a", "erss-changed-b"),
        Set("erss-changed-b")
      )
    }
  }

  // -- attachToExecution: the engine callbacks -------------------------------

  "the OperatorPortResultUriAvailable callback" should
    "persist the URI under the attached execution" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val globalPortId = globalPortIdOf("erss-callback")
      val uri = resultUriOf(executionId, "erss-callback")

      fixture.client.fire(OperatorPortResultUriAvailable(globalPortId, uri))

      val rows = getDSLContext
        .selectFrom(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(executionId.id.toInt))
        .fetch()
      rows.size() shouldBe 1
      rows.get(0).getGlobalPortId shouldBe globalPortId.serializeAsString
      rows.get(0).getResultUri shouldBe uri.toString
    }
  }

  "the ExecutionStateUpdate callback" should
    "stop polling and run one final update when the execution terminates" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-final-snapshot",
      schema,
      (0 until 3).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    storeResult(
      executionId,
      "erss-final-delta",
      schema,
      (0 until 2).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    val plan = planOf(
      physicalOp(executionId, "erss-final-snapshot", PortIdentity() -> OutputMode.SET_SNAPSHOT),
      physicalOp(executionId, "erss-final-delta", PortIdentity() -> OutputMode.SET_DELTA)
    )

    withAttached(plan, executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      polling.cancelCount shouldBe 1
      val resultInfo = fixture.workflowStateStore.resultStore.getState.resultInfo
      resultInfo.keySet.map(_.id) shouldBe Set("erss-final-snapshot", "erss-final-delta")
      // Counts come from the documents themselves, so the two operators must not
      // be reported with the same count.
      resultInfo(OperatorIdentity("erss-final-snapshot")).tupleCount shouldBe 3
      resultInfo(OperatorIdentity("erss-final-delta")).tupleCount shouldBe 2
      // A SET_SNAPSHOT result is replaced wholesale, so its content can change
      // without its count changing; the random change detector is what makes the
      // frontend re-read it. A delta is append-only and needs no such nudge.
      resultInfo(OperatorIdentity("erss-final-snapshot")).changeDetector should not be empty
      resultInfo(OperatorIdentity("erss-final-delta")).changeDetector shouldBe ""
    }
  }

  it should "leave polling running while the execution has not terminated" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.PAUSED))

      polling.cancelCount shouldBe 0
      fixture.workflowStateStore.resultStore.getState.resultInfo shouldBe empty
    }
  }

  "the FatalError callback" should "stop polling" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(FatalError(new RuntimeException("boom")))

      polling.cancelCount shouldBe 1
    }
  }

  it should "tolerate a fatal error raised before polling ever started" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      // A workflow can fail during initialization, i.e. before it reaches RUNNING
      // and creates a polling handle. Without the null guard this is an NPE
      // inside the error path, which would mask the original failure.
      noException should be thrownBy fixture.client.fire(FatalError(new RuntimeException("boom")))
    }
  }

  "attachToExecution" should "cancel the previous execution's polling" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.service.attachToExecution(
        executionId,
        new ExecutionStateStore,
        planOf(),
        fixture.client
      )

      // Two cancels, from two different places: attachToExecution's own guard,
      // and then the freshly registered metadata subscription, which replays the
      // store's current (non-RUNNING) state immediately and cancels on it. The
      // next case pins the guard by removing its contribution.
      polling.cancelCount shouldBe 2
    }
  }

  it should "not re-cancel polling that is already cancelled" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable(initiallyCancelled = true)
      setPollingCancellable(fixture.service, polling)

      fixture.service.attachToExecution(
        executionId,
        new ExecutionStateStore,
        planOf(),
        fixture.client
      )

      // Only the metadata subscription's cancel is left: attachToExecution's guard
      // skips a handle that reports itself already cancelled.
      polling.cancelCount shouldBe 1
    }
  }

  // ---------------------------------------------------------------- fixtures

  /** An AmberClient that records the callbacks the service registers, so tests can fire them. */
  private final class RecordingAmberClient
      extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    private val callbacks = mutable.Map.empty[Class[_], Any => Unit]

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      callbacks(ct.runtimeClass) = callback.asInstanceOf[Any => Unit]
      Disposable.empty()
    }

    /** Delivers `event` to the callback registered for its type; fails if there is none. */
    def fire[T <: AnyRef](event: T): Unit =
      callbacks
        .getOrElse(
          event.getClass,
          fail(s"no callback registered for ${event.getClass.getSimpleName}")
        )
        .apply(event)
  }

  /** A Cancellable that counts cancellations, standing in for AmberRuntime's scheduled poll. */
  private final class RecordingCancellable(initiallyCancelled: Boolean = false)
      extends Cancellable {
    var cancelCount = 0
    private var cancelled = initiallyCancelled

    override def cancel(): Boolean = {
      cancelCount += 1
      cancelled = true
      true
    }

    override def isCancelled: Boolean = cancelled
  }

  private final class Fixture(
      val client: RecordingAmberClient,
      val service: ExecutionResultService,
      val workflowStateStore: WorkflowStateStore
  ) {

    /** Publishes one engine-reported tuple count per operator, as the poller does. */
    def setResultCounts(counts: (String, Int)*): Unit =
      workflowStateStore.resultStore.updateState(_ =>
        WorkflowResultStore(counts.map {
          case (opId, count) => OperatorIdentity(opId) -> OperatorResultMetadata(count)
        }.toMap)
      )
  }

  /** Collects what the result store's diff handler emits, and any error it raises. */
  private final class ResultEvents(workflowStateStore: WorkflowStateStore) {
    private val collected = mutable.ArrayBuffer.empty[TexeraWebSocketEvent]
    val errors: mutable.ArrayBuffer[Throwable] = mutable.ArrayBuffer.empty[Throwable]

    workflowStateStore.resultStore.getWebsocketEventObservable.subscribe(
      (evts: Iterable[TexeraWebSocketEvent]) => collected ++= evts,
      (err: Throwable) => errors += err
    )

    def updates: List[WebResultUpdateEvent] =
      collected.collect { case e: WebResultUpdateEvent => e }.toList
  }

  private def withAttached(physicalPlan: PhysicalPlan, executionId: ExecutionIdentity)(
      body: Fixture => Unit
  ): Unit = {
    val client = new RecordingAmberClient
    val workflowStateStore = new WorkflowStateStore
    val service = new ExecutionResultService(
      WorkflowIdentity(testWid.longValue()),
      testCuid,
      workflowStateStore
    )
    try {
      service.attachToExecution(executionId, new ExecutionStateStore, physicalPlan, client)
      body(new Fixture(client, service, workflowStateStore))
    } finally {
      service.unsubscribeAll()
      client.shutdown()
    }
  }

  /**
    * Runs `request` against a service bound to the seeded workflow and computing
    * unit, so getLatestExecutionId resolves against the rows this spec inserts.
    */
  private def paginate(request: ResultPaginationRequest): PaginatedResultEvent = {
    val service = new ExecutionResultService(
      WorkflowIdentity(testWid.longValue()),
      testCuid,
      new WorkflowStateStore
    )
    service.handleResultPagination(request) match {
      case event: PaginatedResultEvent => event
      case other                       => fail(s"expected a PaginatedResultEvent, got $other")
    }
  }

  private def dataUpdate(update: ExecutionResultService.WebResultUpdate): WebDataUpdate =
    update match {
      case dataUpdate: WebDataUpdate => dataUpdate
      case other                     => fail(s"expected a WebDataUpdate, got $other")
    }

  private def newExecution(): ExecutionIdentity = {
    val execution = new WorkflowExecutions
    execution.setVid(testVid)
    execution.setUid(testUid)
    execution.setCuid(testCuid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("execution-result-instance-test")
    execution.setEnvironmentVersion("test-env")
    executionsDao.insert(execution)
    ExecutionIdentity(execution.getEid.longValue())
  }

  private def globalPortIdOf(operatorId: String): GlobalPortIdentity =
    GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
      PortIdentity(),
      input = false
    )

  /** The external-output result URI shape that getResultUriByLogicalPortId decodes and matches. */
  private def resultUriOf(
      executionId: ExecutionIdentity,
      operatorId: String,
      warehouse: Option[String] = None
  ): URI =
    VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(
        WorkflowIdentity(testWid.longValue()),
        executionId,
        globalPortIdOf(operatorId),
        warehouse
      )
    )

  private def insertResultUri(executionId: ExecutionIdentity, uri: URI): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(
        Integer.valueOf(executionId.id.toInt),
        VFSURIFactory.decodeURI(uri).globalPortId.get.serializeAsString,
        uri.toString
      )
      .execute()

  /** Creates a real Iceberg result table holding `tuples`, and records its URI for `executionId`. */
  private def storeResult(
      executionId: ExecutionIdentity,
      operatorId: String,
      schema: Schema,
      tuples: Seq[Tuple]
  ): URI = {
    val uri = resultUriOf(executionId, operatorId)
    val document = DocumentFactory.createDocument(uri, schema)
    val writer = document.writer("erss").asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()
    insertResultUri(executionId, uri)
    uri
  }

  private def storedResultSize(executionId: ExecutionIdentity, operatorId: String): Long =
    getDSLContext
      .select(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(executionId.id.toInt))
      .and(
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortIdOf(operatorId).serializeAsString)
      )
      .fetchOne(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)

  /**
    * A single-layer physical op whose output ports are declared in the given order --
    * a ListMap, because convertWebResultUpdate reads the FIRST external port's mode.
    */
  private def physicalOp(
      executionId: ExecutionIdentity,
      operatorId: String,
      ports: (PortIdentity, OutputMode)*
  ): PhysicalOp =
    PhysicalOp(
      id = PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
      workflowId = WorkflowIdentity(testWid.longValue()),
      executionId = executionId,
      opExecInitInfo = OpExecInitInfo.Empty,
      outputPorts = ListMap.from(ports.map {
        case (portId, mode) =>
          portId -> (OutputPort(id = portId, mode = mode), List.empty, Right(Schema()))
      })
    )

  private def planOf(ops: PhysicalOp*): PhysicalPlan = PhysicalPlan(ops.toSet, Set.empty)

  /**
    * Sets the private polling handle. AmberRuntime creates it once the execution
    * reaches RUNNING, from an ActorSystem it keeps in a private var that only a
    * started pekko cluster fills in; the terminal-state, fatal-error and re-attach
    * paths are all defined by what they do to an already-created handle.
    */
  private def setPollingCancellable(
      service: ExecutionResultService,
      cancellable: Cancellable
  ): Unit = {
    val field = classOf[ExecutionResultService].getDeclaredField("resultUpdateCancellable")
    field.setAccessible(true)
    field.set(service, cancellable)
  }
}
