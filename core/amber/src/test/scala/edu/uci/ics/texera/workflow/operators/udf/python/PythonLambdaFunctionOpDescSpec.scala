package edu.uci.ics.texera.workflow.operators.udf.python

import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class PythonLambdaFunctionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("column_str", AttributeType.STRING),
    new Attribute("column_int", AttributeType.INTEGER),
    new Attribute("column_bool", AttributeType.BOOLEAN)
  )

  var opDesc: PythonLambdaFunctionOpDesc = _

  before {
    opDesc = new PythonLambdaFunctionOpDesc()
  }

  it should "add one new column into schema successfully" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "newColumn1",
        AttributeType.STRING
      )
    )
    val outputSchema = opDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 4)
  }

  it should "add multiple new columns into schema successfully" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "newColumn1",
        AttributeType.STRING
      ),
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_int']",
        "newColumn2",
        AttributeType.INTEGER
      )
    )
    val outputSchema = opDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 5)
  }

  it should "build successfully when there is no new column but with modifying the existing column" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "column_str",
        "tuple_['column_str'] + hello",
        "",
        AttributeType.STRING
      )
    )
    val outputSchema = opDesc.getOutputSchema(Array(schema))
    assert(outputSchema.getAttributes.length == 3)
  }

  it should "raise exception if the new column name already exists" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "column_str",
        AttributeType.STRING
      )
    )

    assertThrows[RuntimeException] {
      opDesc.getOutputSchema(Array(schema))
    }

  }
}
