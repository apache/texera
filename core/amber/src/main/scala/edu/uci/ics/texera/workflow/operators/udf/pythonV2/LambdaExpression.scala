package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class LambdaExpression(
    expression: String,
    attributeName: String,
    attributeType: AttributeType,
    inputSchema: Schema
) {
  def eval(): String = {

    /**
      * For example, expression = `Unit Price` * 2 + 1
      * any attribute should be wrapped by ``, and the schema must contain this attribute
      * for now, only numerical types such as Int, Long, Double support lambda expression
      * String, Boolean types only support assignment operation
      */
    attributeType match {
      case AttributeType.STRING  => generatePythonCodeString()
      case AttributeType.BOOLEAN => generatePythonCodeBoolean()
      case AttributeType.LONG | AttributeType.INTEGER | AttributeType.DOUBLE =>
        generatePythonCodeNumeric()
      case _ => throw new RuntimeException(s"unsupported attribute type: $attributeType")
    }
  }

  private def generatePythonCodeString(): String = {
    s"""        tuple_['$attributeName'] = "$expression"\n"""
  }

  private def generatePythonCodeBoolean(): String = {
    val booleanValue =
      if (expression.equalsIgnoreCase("true")) "True"
      else if (expression.equalsIgnoreCase("false")) "False"
      else throw new RuntimeException("Boolean value can only be true or false")
    s"""        tuple_['$attributeName'] = $booleanValue\n"""
  }

  private def generatePythonCodeNumeric(): String = {
    val tokens = findTokens(expression)
    var newExpression: String = expression
    tokens.foreach { token =>
      if (!inputSchema.containsAttribute(token)) {
        throw new RuntimeException(s"Column name $token doesn't exist!")
      }
      if (!isNumeric(inputSchema.getAttribute(token).getType)) {
        throw new RuntimeException(s"Column name $token isn't a numerical data type")
      }
      newExpression = replaceToken(newExpression, token)
    }
    s"""        tuple_['$attributeName'] = $newExpression\n"""
  }

  def findTokens(s: String): Seq[String] = {
    val pattern = "`([^`]+)`".r
    pattern.findAllMatchIn(s).map(_.group(1)).toSeq
  }

  def replaceToken(s: String, token: String): String = {
    val pattern = s"`$token`"
    s.replaceAll(pattern, s"tuple_['$token']")
  }

  def isNumeric(x: AttributeType): Boolean =
    x match {
      case AttributeType.INTEGER | AttributeType.LONG | AttributeType.DOUBLE => true
      case _                                                                 => false
    }
}
