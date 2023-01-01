package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, AttributeTypeUtils, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class LambdaExpressionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("column1", AttributeType.STRING),
    new Attribute("column2", AttributeType.INTEGER),
    new Attribute("column3", AttributeType.BOOLEAN)
  )
  var lambdaExpressionOpDesc: LambdaExpressionOpDesc = _

  before {
    lambdaExpressionOpDesc = new LambdaExpressionOpDesc()
  }

  it should "take in new columns" in {
    lambdaExpressionOpDesc.newColumns ++= List(
      new Attribute("newColumn1", AttributeType.STRING),
      new Attribute("newColumn2", AttributeType.STRING),
    )

    assert(lambdaExpressionOpDesc.newColumns.length == 2)

  }

  it should "add one new column into schema successfully" in {
    lambdaExpressionOpDesc.newColumns ++= List(
      new Attribute("newColumn1", AttributeType.STRING),
    )

    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 4)

  }

  it should "add multiple new columns into schema successfully" in {
    lambdaExpressionOpDesc.newColumns ++= List(
      new Attribute("newColumn1", AttributeType.STRING),
      new Attribute("newColumn2", AttributeType.STRING),
    )

    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 5)

  }

  it should "build without new columns successfully" in {
    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 3)

  }

  it should "raise exception if the column already exists" in {
    lambdaExpressionOpDesc.newColumns ++= List(
      new Attribute("column1", AttributeType.STRING),
    )

    assertThrows[RuntimeException] {
      lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    }

  }
}
