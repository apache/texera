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
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.OperatorPortResultUriAvailable
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  OPERATOR_PORT_EXECUTIONS,
  USER,
  WORKFLOW,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.service.ExecutionResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebDataUpdate,
  WebPaginationUpdate
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters._

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

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    val user = new User
    user.setUid(testUid)
    user.setName("execution-result-test-user")
    user.setEmail(s"u$testUid@example.com")
    user.setPassword("password")
    new UserDao(getDSLContext.configuration()).insert(user)

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
}
