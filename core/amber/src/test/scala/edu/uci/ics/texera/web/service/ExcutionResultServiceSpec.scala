package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

class ExcutionResultServiceSpec extends AnyFlatSpec with Matchers {

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
    val shortBinaryData = List(ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5)))
    val longBinaryData = List(
      ByteBuffer.wrap(Array.tabulate[Byte](50)(_.toByte)),
      ByteBuffer.wrap(Array.tabulate[Byte](50)(i => (i + 50).toByte))
    )

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
      startWith("bytes'") and
        include("01 02 03 04 05") and
        include("(length: 5)")
    )

    // Check long binary representation
    val longBinaryString = jsonNode.get("longBinaryCol").asText()
    longBinaryString should (
      startWith("bytes'") and
        include("...") and
        include("(length: 100)")
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

    // Empty binary list
    val emptyBinaryData = List(ByteBuffer.wrap(Array[Byte]()))

    val tuple = Tuple
      .builder(schema)
      .add("emptyBinary", AttributeType.BINARY, emptyBinaryData)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    val emptyBinaryString = jsonNode.get("emptyBinary").asText()
    emptyBinaryString should include("(length: 0)")
  }

  it should "handle binary data with single ByteBuffer" in {
    val attributes = List(
      new Attribute("singleBufferBinary", AttributeType.BINARY)
    )
    val schema = new Schema(attributes)

    // Create binary data with a single ByteBuffer
    val singleBufferData = List(ByteBuffer.wrap("Hello, world!".getBytes()))

    val tuple = Tuple
      .builder(schema)
      .add("singleBufferBinary", AttributeType.BINARY, singleBufferData)
      .build()

    val result = ExecutionResultService.convertTuplesToJson(List(tuple))

    result should have size 1
    val jsonNode = result.head

    val binaryString = jsonNode.get("singleBufferBinary").asText()
    binaryString should (
      startWith("bytes'") and
        include("(length: 13)") // "Hello, world!" is 13 bytes
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

    val binaryData1 = List(ByteBuffer.wrap(Array[Byte](10, 20, 30)))
    val binaryData2 = List(ByteBuffer.wrap(Array[Byte](40, 50, 60)))

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
      include("0A 14 1E") and // Hex representation of 10, 20, 30
        include("(length: 3)")
    )

    val binaryString2 = jsonNode.get("binaryField2").asText()
    binaryString2 should (
      include("28 32 3C") and // Hex representation of 40, 50, 60
        include("(length: 3)")
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
}
