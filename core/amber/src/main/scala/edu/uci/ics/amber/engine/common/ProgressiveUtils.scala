package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema, Tuple}

object ProgressiveUtils {

  // boolean attribute to indicate insertion / retraction
  // true  indicates insertion  (+)
  // false indicates retraction (-)
  val insertRetractFlagAttr = new Attribute("__internal_is_insertion", AttributeType.BOOLEAN)

  def addInsertionFlag(tuple: Tuple, outputSchema: Schema): Tuple = {
    assert(!tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName))
    Tuple.builder(outputSchema).add(insertRetractFlagAttr, true).add(tuple).build()
  }

  def addRetractionFlag(tuple: Tuple, outputSchema: Schema): Tuple = {
    assert(!tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName))
    Tuple.builder(outputSchema).add(insertRetractFlagAttr, false).add(tuple).build()
  }

  def isInsertion(tuple: Tuple): Boolean = {
    if (tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName)) {
      tuple.getField[Boolean](insertRetractFlagAttr.getName)
    } else {
      true
    }
  }

  def getTupleFlagAndValue(
      tuple: Tuple
  ): (Boolean, Tuple) = {
    (
      isInsertion(tuple), {
        val originalSchema = tuple.getSchema
        val schema = originalSchema.getPartialSchema(
          originalSchema.getAttributeNames.filterNot(_ == insertRetractFlagAttr.getName)
        )
        Tuple.builder(schema).add(tuple, isStrictSchemaMatch = false).build()
      }
    )
  }

}
