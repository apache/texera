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
}
