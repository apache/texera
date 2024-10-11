package edu.uci.ics.texera.workflow.operators.projection

import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

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
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("fields2", "f2")
    )

    assert(projectionOpDesc.attributes.length == 2)

  }

  it should "filter schema correctly" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("field2", "f2")
    )
    val outputSchema = projectionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 2)

  }

  it should "reorder schema" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema = projectionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("f2") == 0)
    assert(outputSchema.getIndex("f1") == 1)

  }

  it should "raise RuntimeException on non-existing fields" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field---5", "f5"),
      new AttributeUnit("field---6", "f6")
    )
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

    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )

    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getOutputSchema(Array(schema, schema))
    }

  }

  it should "raise RuntimeException on duplicate alias" in {

    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "f"),
      new AttributeUnit("field1", "f")
    )
    assertThrows[RuntimeException] {
      projectionOpDesc.getOutputSchema(Array(schema))
    }
  }

  it should "allow alias to be optional" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("field2", "")
    )
    val outputSchema = projectionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 2)

  }

}
