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

package org.apache.texera.amber.core.tuple

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.jdk.CollectionConverters._

class TupleUtilsSpec extends AnyFlatSpec with Matchers {

  private def schemaOf(specs: (String, AttributeType)*): Schema =
    new Schema(specs.map { case (name, tpe) => new Attribute(name, tpe) }: _*)

  private def tupleOf(fields: (String, AttributeType, Any)*): Tuple = {
    val schema = schemaOf(fields.map { case (n, t, _) => n -> t }: _*)
    fields
      .foldLeft(Tuple.builder(schema)) { case (b, (n, t, v)) => b.add(n, t, v) }
      .build()
  }

  private def singleFieldJson(
      name: String,
      tpe: AttributeType,
      value: Any
  ): JsonNode = {
    val result = TupleUtils.convertTuplesToJson(List(tupleOf((name, tpe, value))))
    result should have size 1
    result.head.get(name)
  }

  // --- tuple2json ------------------------------------------------------------

  "TupleUtils.tuple2json" should "emit one JSON field per schema attribute, in the schema's declared order" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER, "name" -> AttributeType.STRING)
    val node = TupleUtils.tuple2json(schema, Array[Any](Int.box(7), "alice"))
    // Field iteration order on Jackson ObjectNode follows insertion order,
    // which mirrors the schema's getAttributeNames order.
    assert(node.fieldNames().asScala.toList == List("id", "name"))
    assert(node.get("id").asInt() == 7)
    assert(node.get("name").asText() == "alice")
  }

  it should "emit JSON null for null field values" in {
    val schema = schemaOf("v" -> AttributeType.STRING)
    val node = TupleUtils.tuple2json(schema, Array[Any](null))
    assert(node.get("v").isNull)
  }

  it should "respect schema.getIndex when fieldVals is laid out positionally" in {
    // Re-ordering the schema must change which slot of fieldVals each
    // attribute pulls from, because tuple2json indexes fieldVals via
    // schema.getIndex(attrName).
    val schema = schemaOf("b" -> AttributeType.STRING, "a" -> AttributeType.STRING)
    val node = TupleUtils.tuple2json(schema, Array[Any]("first", "second"))
    assert(node.get("b").asText() == "first")
    assert(node.get("a").asText() == "second")
  }

  it should "produce an empty object for an empty schema" in {
    val node = TupleUtils.tuple2json(new Schema(), Array.empty[Any])
    assert(node.size() == 0)
  }

  // --- convertTuplesToJson ---------------------------------------------------

  "convertTuplesToJson" should "convert tuples with various field types correctly" in {
    // Create a string longer than maxStringLength (100)
    val longString = "a" * 150

    // Create binary data
    val shortBinaryData = Array[Byte](1, 2, 3, 4, 5)
    val longBinaryData = Array.tabulate[Byte](100)(_.toByte)

    // Create a tuple with all the test data
    val tuple = tupleOf(
      ("stringCol", AttributeType.STRING, "regular string"),
      ("intCol", AttributeType.INTEGER, 42),
      ("boolCol", AttributeType.BOOLEAN, true),
      ("nullCol", AttributeType.ANY, null),
      ("longStringCol", AttributeType.STRING, longString),
      ("shortBinaryCol", AttributeType.BINARY, shortBinaryData),
      ("longBinaryCol", AttributeType.BINARY, longBinaryData)
    )

    // Convert to JSON
    val result = TupleUtils.convertTuplesToJson(List(tuple))

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
    val result = TupleUtils.convertTuplesToJson(List())
    result shouldBe empty
  }

  it should "handle collections with multiple tuples" in {
    // Create multiple tuples
    val tuple1 = tupleOf(("id", AttributeType.INTEGER, 1), ("name", AttributeType.STRING, "Alice"))
    val tuple2 = tupleOf(("id", AttributeType.INTEGER, 2), ("name", AttributeType.STRING, "Bob"))

    // Convert to JSON
    val results = TupleUtils.convertTuplesToJson(List(tuple1, tuple2))

    // Verify the results
    results should have size 2
    results.head.get("id").asInt() shouldBe 1
    results.head.get("name").asText() shouldBe "Alice"
    results(1).get("id").asInt() shouldBe 2
    results(1).get("name").asText() shouldBe "Bob"
  }

  it should "handle string exactly at the maximum length" in {
    // Create string exactly at maxStringLength (100)
    val exactLengthString = "x" * 100
    val text =
      singleFieldJson("exactLengthString", AttributeType.STRING, exactLengthString).asText()

    text shouldBe exactLengthString
    text should have length 100
  }

  it should "handle empty binary data" in {
    val emptyBinaryString =
      singleFieldJson("emptyBinary", AttributeType.BINARY, Array[Byte]()).asText()
    emptyBinaryString should include("size = 0 bytes")
  }

  it should "handle binary data from a single byte array" in {
    val binaryString =
      singleFieldJson("singleBufferBinary", AttributeType.BINARY, "Hello, world!".getBytes())
        .asText()
    binaryString should (
      startWith("<binary") and
        include("size = 13 bytes") // "Hello, world!" is 13 bytes
    )
  }

  it should "handle various numeric types correctly" in {
    val tuple = tupleOf(
      ("intValue", AttributeType.INTEGER, Int.MaxValue),
      ("doubleValue", AttributeType.DOUBLE, 3.14159),
      ("longValue", AttributeType.LONG, Long.MaxValue)
    )

    val result = TupleUtils.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    jsonNode.get("intValue").asInt() shouldBe Int.MaxValue
    jsonNode.get("doubleValue").asDouble() shouldBe 3.14159
    jsonNode.get("longValue").asLong() shouldBe Long.MaxValue
  }

  it should "handle multiple binary fields within the same tuple" in {
    val tuple = tupleOf(
      ("binaryField1", AttributeType.BINARY, Array[Byte](10, 20, 30)),
      ("binaryField2", AttributeType.BINARY, Array[Byte](40, 50, 60))
    )

    val result = TupleUtils.convertTuplesToJson(List(tuple))

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
    val schema = schemaOf("longStringCol" -> AttributeType.STRING)

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
    val resultsDefault = TupleUtils.convertTuplesToJson(List(tuple1, tuple2))

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
      TupleUtils.convertTuplesToJson(List(tuple1, tuple2), true)

    // Verify no truncation happens
    resultsVisualization(0).get("longStringCol").asText() shouldBe longString
    resultsVisualization(0).get("longStringCol").asText() should have length 150

    resultsVisualization(1).get("longStringCol").asText() shouldBe htmlVisualizationString
    resultsVisualization(1)
      .get("longStringCol")
      .asText() should have length htmlVisualizationString.length
  }

  it should "handle direct comparison between non-visualization and visualization mode" in {
    // Create strings of various lengths
    val shortString = "short string" // under maxStringLength
    val exactLengthString = "x" * 100 // exactly maxStringLength
    val longString = "y" * 200 // over maxStringLength

    val tuple = tupleOf(
      ("col1", AttributeType.STRING, shortString),
      ("col2", AttributeType.STRING, exactLengthString),
      ("col3", AttributeType.STRING, longString)
    )

    // Convert with both modes
    val resultDefault = TupleUtils.convertTuplesToJson(List(tuple), false)
    val resultVisualization = TupleUtils.convertTuplesToJson(List(tuple), true)

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
    val schema = schemaOf("value" -> AttributeType.STRING)

    // Create a collection with both short and long strings
    val tuples = List(
      Tuple.builder(schema).add("value", AttributeType.STRING, "short").build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "a" * 150).build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "medium length").build(),
      Tuple.builder(schema).add("value", AttributeType.STRING, "b" * 200).build()
    )

    // Test with visualization flag true
    val resultsVisualization = TupleUtils.convertTuplesToJson(tuples, true)

    // All strings should remain intact
    resultsVisualization(0).get("value").asText() shouldBe "short"
    resultsVisualization(1).get("value").asText() shouldBe "a" * 150
    resultsVisualization(2).get("value").asText() shouldBe "medium length"
    resultsVisualization(3).get("value").asText() shouldBe "b" * 200

    // Test with visualization flag false (default)
    val resultsDefault = TupleUtils.convertTuplesToJson(tuples)

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
    // 1 byte = 8 bits <= 10 (leading) + 3 (trailing), so no truncation/ellipsis.
    val text = singleFieldJson("b", AttributeType.BINARY, Array[Byte](5)).asText()
    text shouldBe "<binary 00000101, size = 1 bytes>"
  }

  it should "render leading-10 and trailing-3 preview bits with an ellipsis for larger binary fields" in {
    // 3 bytes = 24 bits > 13: preview = first 10 bits + "..." + last 3 bits.
    // bytes 0xFF,0x00,0xAA -> "11111111 00000000 10101010"
    //   leading 10 bits  = "1111111100"
    //   trailing 3 bits  = "010"
    val bytes = Array[Byte](0xff.toByte, 0x00.toByte, 0xaa.toByte)
    val text = singleFieldJson("b", AttributeType.BINARY, bytes).asText()
    text shouldBe "<binary 1111111100...010, size = 3 bytes>"
  }

  // TIMESTAMP passes through to the shared objectMapper unchanged.
  it should "pass timestamp fields through unchanged to the shared serializer" in {
    val ts = Timestamp.valueOf("2023-01-15 08:30:45.123")
    val node = singleFieldJson("ts", AttributeType.TIMESTAMP, ts)
    node shouldBe objectMapper.valueToTree[JsonNode](ts)
  }
}
