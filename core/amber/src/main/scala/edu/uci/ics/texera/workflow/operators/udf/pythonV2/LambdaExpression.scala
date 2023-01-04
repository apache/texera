package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

class LambdaExpression(expression: String, attributeName: String, attributeType: AttributeType) {
  def eval(): String = {
    /**
      * For example, expression = `Unit Price` * 2 + 1
      * any attribute should be wrapped by ``, and the schema must contain this attribute
      * for now, only numerical types such as Int, Long, Double support lambda expression
      * String, Boolean types only support assignment operation
      */
    attributeType match {
      case AttributeType.STRING => generatePythonCodeString()
      case AttributeType.BOOLEAN => generatePythonCodeBoolean()
      case AttributeType.LONG | AttributeType.INTEGER | AttributeType.DOUBLE => generatePythonCodeNumeric()
      case _ => throw new RuntimeException(s"unsupported attribute type: $attributeType")
    }
  }

  private def generatePythonCodeString(): String = {
    s"""        tuple_["$attributeName"] = "$expression"\n"""
  }

  private def generatePythonCodeBoolean(): String = {
    val booleanValue = if (expression.equalsIgnoreCase("true")) "True" else if (expression.equalsIgnoreCase("false")) "False" else throw new RuntimeException("Boolean value can only be true or false")
    s"""        tuple_["$attributeName"] = $booleanValue\n"""
  }

  private def generatePythonCodeNumeric(): String = {
    s"""        tuple_["$attributeName"] = Utils.evaluate(tuple_, "$expression")\n"""
  }
}