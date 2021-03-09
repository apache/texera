package edu.uci.ics.texera.workflow.common

import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

import scala.util.control.Exception.allCatch

object TypeUtils {
  def inferField (attributeType: AttributeType, fieldValue: String): AttributeType={
    attributeType match {
      case AttributeType.STRING  => tryParseString()
      case AttributeType.BOOLEAN => tryParseBoolean(fieldValue)
      case AttributeType.DOUBLE  => tryParseDouble(fieldValue)
      case AttributeType.LONG    => tryParseLong(fieldValue)
      case AttributeType.INTEGER => tryParseInteger(fieldValue)
      case _                     => tryParseString()
    }
  }

  private def tryParseInteger(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toInt match {
      case Some(_) => AttributeType.INTEGER
      case None    => tryParseLong(fieldValue)
    }
  }

  private def tryParseLong(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toLong match {
      case Some(_) => AttributeType.LONG
      case None    => tryParseDouble(fieldValue)
    }
  }

  private def tryParseDouble(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toDouble match {
      case Some(_) => AttributeType.DOUBLE
      case None    => tryParseBoolean(fieldValue)
    }
  }
  private def tryParseBoolean(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toBoolean match {
      case Some(_) => AttributeType.BOOLEAN
      case None => tryParseString()
    }
  }
  private def tryParseString(): AttributeType = {
      AttributeType.STRING
    }


}
