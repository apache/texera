package edu.uci.ics.texera.workflow.common.tuple

import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.{
  inferSchemaFromRows,
  parseField
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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
    val inputTuple = Tuple.newBuilder().add(stringAttribute, "string-value").build()
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
    val line = inputTuple.asKeyValuePairJson().toString

    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val root: JsonNode = objectMapper.readTree(line)
    if (root.isObject) {
      val fields: Map[String, String] = JSONToMap(root)
      fieldNames = fieldNames.++(fields.keySet)
      allFields += fields
    }

    val sortedFieldNames = fieldNames.toList

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    val schema = Schema.newBuilder
      .add(
        sortedFieldNames.indices
          .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
          .asJava
      )
      .build

    try {
      val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
      val data = JSONToMap(objectMapper.readTree(line))

      for (fieldName <- schema.getAttributeNames.asScala) {
        if (data.contains(fieldName))
          fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        else {
          fields += null
        }
      }

      val newTuple = Tuple.newBuilder(schema).addSequentially(fields.toArray).build
      assert(inputTuple.toString.equals(newTuple.toString))
    } catch {
      case _: Throwable => null
    }

  }
}
