package edu.uci.ics.texera.workflow.common.tuple

import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.{json2tuple, tuple2json}
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.flatspec.AnyFlatSpec

class TupleSpec extends AnyFlatSpec {
  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

  val capitalizedStringAttribute = new Attribute("COL-string", AttributeType.STRING)

  it should "create a tuple with capitalized attributeName" in {

    val schema = Schema.newBuilder().add(capitalizedStringAttribute).build()
    val tuple = Tuple.newBuilder(schema).add(capitalizedStringAttribute, "string-value").build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")

  }

  it should "create a tuple with capitalized attributeName, using addSequentially" in {
    val schema = Schema.newBuilder().add(capitalizedStringAttribute).build()
    val tuple = Tuple.newBuilder(schema).addSequentially(Array("string-value")).build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")
  }

  it should "create a tuple using new builder, based on another tuple using old builder" in {
    val schema = Schema.newBuilder().add(stringAttribute).build()
    val inputTuple = Tuple.newBuilder(schema).addSequentially(Array("string-value")).build()
    val newTuple = Tuple.newBuilder(inputTuple.getSchema).add(inputTuple).build()

    assert(newTuple.size == inputTuple.size)
  }

  it should "fail when unknown attribute is added to tuple" in {
    val schema = Schema.newBuilder().add(stringAttribute).build()
    assertThrows[TupleBuildingException] {
      Tuple.newBuilder(schema).add(integerAttribute, 1)
    }
  }

  it should "fail when tuple does not conform to complete schema" in {
    val schema = Schema.newBuilder().add(stringAttribute).add(integerAttribute).build()
    assertThrows[TupleBuildingException] {
      Tuple.newBuilder(schema).add(integerAttribute, 1).build()
    }
  }

  it should "fail when entire tuple passed in has extra attributes" in {
    val inputSchema =
      Schema.newBuilder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()
    val inputTuple = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema.newBuilder().add(stringAttribute).add(integerAttribute).build()
    assertThrows[TupleBuildingException] {
      Tuple.newBuilder(outputSchema).add(inputTuple).build()
    }
  }

  it should "not fail when entire tuple passed in has extra attributes and strictSchemaMatch is false" in {
    val inputSchema =
      Schema.newBuilder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()
    val inputTuple = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema.newBuilder().add(stringAttribute).add(integerAttribute).build()
    val outputTuple = Tuple.newBuilder(outputSchema).add(inputTuple, false).build()

    // This is the important test. Input tuple has 3 attributes but output tuple has only 2
    // It's because of isStrictSchemaMatch=false
    assert(outputTuple.size == 2);
  }

  it should "produce identical strings" in {
    val inputSchema =
      Schema.newBuilder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()
    val inputTuple = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val line = tuple2json(inputTuple)
    val newTuple = json2tuple(line)
    assert(line == tuple2json(newTuple))

  }
}
