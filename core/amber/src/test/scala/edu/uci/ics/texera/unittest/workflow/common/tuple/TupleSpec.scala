package edu.uci.ics.texera.unittest.workflow.common.tuple

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.flatspec.AnyFlatSpec

class TupleSpec extends AnyFlatSpec {
  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

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
}
