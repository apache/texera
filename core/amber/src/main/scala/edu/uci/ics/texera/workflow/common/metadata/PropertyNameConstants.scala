package edu.uci.ics.texera.workflow.common.metadata

/**
  * PropertyNameConstants defines the key names
  * in the JSON representation of each operator.
  *
  * @author Zuozhi Wang
  */
object PropertyNameConstants { // logical plan property names
  final val OPERATOR_ID = "operatorID"
  final val OPERATOR_TYPE = "operatorType"
  final val ORIGIN_OPERATOR_ID = "origin"
  final val DESTINATION_OPERATOR_ID = "destination"
  final val OPERATOR_LIST = "operators"
  final val OPERATOR_LINK_LIST = "links"
  // common operator property names
  final val ATTRIBUTE_NAMES = "attributes"
  final val ATTRIBUTE_NAME = "attribute"
  final val RESULT_ATTRIBUTE_NAME = "resultAttribute"
  final val SPAN_LIST_NAME = "spanListName"
  final val TABLE_NAME = "tableName"
}
