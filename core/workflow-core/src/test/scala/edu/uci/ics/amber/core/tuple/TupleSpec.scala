package edu.uci.ics.amber.core.tuple

import edu.uci.ics.amber.core.tuple.TupleUtils.{json2tuple, tuple2json}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class TupleSpec extends AnyFlatSpec {
  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)
  val longAttribute = new Attribute("col-long", AttributeType.LONG)
  val doubleAttribute = new Attribute("col-double", AttributeType.DOUBLE)
  val timestampAttribute = new Attribute("col-timestamp", AttributeType.TIMESTAMP)
  val binaryAttribute = new Attribute("col-binary", AttributeType.BINARY)

  val capitalizedStringAttribute = new Attribute("COL-string", AttributeType.STRING)

  it should "create a tuple with capitalized attributeName" in {

    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).add(capitalizedStringAttribute, "string-value").build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")

  }

  it should "create a tuple with capitalized attributeName, using addSequentially" in {
    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")
  }

  it should "create a tuple using new builder, based on another tuple using old builder" in {
    val schema = Schema().add(stringAttribute)
    val inputTuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    val newTuple = Tuple.builder(inputTuple.getSchema).add(inputTuple).build()

    assert(newTuple.length == inputTuple.length)
  }

  it should "fail when unknown attribute is added to tuple" in {
    val schema = Schema().add(stringAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1)
    }
  }

  it should "fail when tuple does not conform to complete schema" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1).build()
    }
  }

  it should "fail when entire tuple passed in has extra attributes" in {
    val inputSchema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(outputSchema).add(inputTuple).build()
    }
  }

  it should "not fail when entire tuple passed in has extra attributes and strictSchemaMatch is false" in {
    val inputSchema =
      Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    val outputTuple = Tuple.builder(outputSchema).add(inputTuple, false).build()

    // This is the important test. Input tuple has 3 attributes but output tuple has only 2
    // It's because of isStrictSchemaMatch=false
    assert(outputTuple.length == 2);
  }

  it should "produce identical strings" in {
    val inputSchema =
      Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val line = tuple2json(inputTuple)
    val newTuple = json2tuple(line)
    assert(line == tuple2json(newTuple))

  }

  it should "calculate hash" in {
    val inputSchema =
      Schema()
        .add(integerAttribute)
        .add(stringAttribute)
        .add(boolAttribute)
        .add(longAttribute)
        .add(doubleAttribute)
        .add(timestampAttribute)
        .add(binaryAttribute)

    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 922323)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .add(longAttribute, 1123213213213L)
      .add(doubleAttribute, 214214.9969346)
      .add(timestampAttribute, new Timestamp(100000000L))
      .add(binaryAttribute, Array[Byte](104, 101, 108, 108, 111))
      .build()
    assert(inputTuple.hashCode() == -1335416166)

    val inputTuple2 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 0)
      .add(stringAttribute, "")
      .add(boolAttribute, false)
      .add(longAttribute, 0L)
      .add(doubleAttribute, 0.0)
      .add(timestampAttribute, new Timestamp(0L))
      .add(binaryAttribute, Array[Byte]())
      .build()
    assert(inputTuple2.hashCode() == -1409761483)

    val inputTuple3 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, null)
      .add(stringAttribute, null)
      .add(boolAttribute, null)
      .add(longAttribute, null)
      .add(doubleAttribute, null)
      .add(timestampAttribute, null)
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple3.hashCode() == 1742810335)

    val inputTuple4 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, -3245763)
      .add(stringAttribute, "\n\r\napple")
      .add(boolAttribute, true)
      .add(longAttribute, -8965536434247L)
      .add(doubleAttribute, 1 / 3.0d)
      .add(timestampAttribute, new Timestamp(-1990))
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple4.hashCode() == -592643630)

    val inputTuple5 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, Int.MaxValue)
      .add(stringAttribute, new String())
      .add(boolAttribute, true)
      .add(longAttribute, Long.MaxValue)
      .add(doubleAttribute, 7 / 17.0d)
      .add(timestampAttribute, new Timestamp(1234567890L))
      .add(binaryAttribute, Array.fill[Byte](4097)('o'))
      .build()
    assert(inputTuple5.hashCode() == -2099556631)
  }
}
