package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class LambdaExpressionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("column_str", AttributeType.STRING),
    new Attribute("column_int", AttributeType.INTEGER),
    new Attribute("column_bool", AttributeType.BOOLEAN)
  )

  var lambdaExpressionOpDesc: LambdaExpressionOpDesc = _

  before {
    lambdaExpressionOpDesc = new LambdaExpressionOpDesc()
  }

  it should "take in new columns" in {
    lambdaExpressionOpDesc.newAttributeUnits ++= List(
      new NewAttributeUnit("newColumn1", AttributeType.STRING, ""),
      new NewAttributeUnit("newColumn2", AttributeType.STRING, "")
    )

    assert(lambdaExpressionOpDesc.newAttributeUnits.length == 2)

  }

  it should "add one new column into schema successfully" in {
    lambdaExpressionOpDesc.newAttributeUnits ++= List(
      new NewAttributeUnit("newColumn", AttributeType.STRING, "")
    )

    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 4)

  }

  it should "add multiple new columns into schema successfully" in {
    lambdaExpressionOpDesc.newAttributeUnits ++= List(
      new NewAttributeUnit("newColumn1", AttributeType.STRING, ""),
      new NewAttributeUnit("newColumn2", AttributeType.STRING, "")
    )

    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 5)

  }

  it should "build successfully when there is no new column" in {
    val outputSchema = lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.size() == 3)
  }

  it should "raise exception if the column already exists" in {
    lambdaExpressionOpDesc.newAttributeUnits ++= List(
      new NewAttributeUnit("column_str", AttributeType.STRING, "")
    )

    assertThrows[RuntimeException] {
      lambdaExpressionOpDesc.getOutputSchema(Array(schema))
    }

  }

  it should "raise exception if the column name in the expression doesn't exist" in {
    assertThrows[RuntimeException] {
      new LambdaExpression(
        "`column_not_exist`",
        "column_int",
        AttributeType.INTEGER,
        schema
      ).eval()
    }
  }

  it should "raise exception if the column name in the expression isn't a numerical type" in {
    assertThrows[RuntimeException] {
      new LambdaExpression(
        "`column_str` * 100",
        "column_int",
        AttributeType.INTEGER,
        schema
      ).eval()
    }
  }

  it should "raise exception if the value assigned to a boolean attribute is not True or False" in {
    assertThrows[RuntimeException] {
      new LambdaExpression(
        "Not a boolean value",
        "column_bool",
        AttributeType.BOOLEAN,
        schema
      ).eval()
    }
  }
}
