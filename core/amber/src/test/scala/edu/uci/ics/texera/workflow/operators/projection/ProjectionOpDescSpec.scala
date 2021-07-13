package edu.uci.ics.texera.workflow.operators.projection

import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
class ProjectionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.INTEGER),
    new Attribute("field3", AttributeType.BOOLEAN)
  )
  var projectionOpDesc: ProjectionOpDesc = _

  before {
    projectionOpDesc = new ProjectionOpDesc()
  }

  it should "take in attribute names" in {

    projectionOpDesc.attributes ++= List("field1", "field2")

    assert(projectionOpDesc.attributes.length == 2)

  }

  it should "filter schema correctly" in {
    projectionOpDesc.attributes ++= List("field1", "field2")
    val outputSchema = projectionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 2)

  }

  it should "reorder schema" in {
    projectionOpDesc.attributes ++= List("field2", "field1")
    val outputSchema = projectionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("field2") == 0)
    assert(outputSchema.getIndex("field1") == 1)

  }

  it should "raise RuntimeException on non-existing fields" in {
    projectionOpDesc.attributes ++= List("field---5", "field---6")
    assertThrows[RuntimeException] {
      projectionOpDesc.getOutputSchema(Array(schema))
    }

  }

  it should "raise IllegalArgumentException on empty attributes" in {

    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getOutputSchema(Array(schema))
    }

  }

  it should "raise IllegalArgumentException with multiple input source Schema" in {
    projectionOpDesc.attributes ++= List("field1", "field2")
    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getOutputSchema(Array(schema, schema))
    }

  }

}
