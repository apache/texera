package edu.uci.ics.texera.workflow.common.metadata

object OperatorGroupConstants {
  final val INPUT_GROUP = "Data Input"
  final val DATABASE_GROUP = "Database Connector"
  final val SEARCH_GROUP = "Search"
  final val MACHINE_LEARNING_GROUP = "Machine Learning"
  final val JOIN_GROUP = "Join"
  final val SET_GROUP = "Set"
  final val UTILITY_GROUP = "Utilities"
  final val API_GROUP = "External API"
  final val UDF_GROUP = "User-defined Functions"
  final val VISUALIZATION_GROUP = "Visualization"

  /**
    * The order of the groups to show up in the frontend operator panel.
    * The order numbers are relative.
    */
  final val OperatorGroupOrderList: List[GroupInfo] = List(
    GroupInfo(INPUT_GROUP, 0),
    GroupInfo(DATABASE_GROUP, 1),
    GroupInfo(SEARCH_GROUP, 2),
    GroupInfo(MACHINE_LEARNING_GROUP, 3),
    GroupInfo(JOIN_GROUP, 4),
    GroupInfo(SET_GROUP, 5),
    GroupInfo(UTILITY_GROUP, 6),
    GroupInfo(API_GROUP, 7),
    GroupInfo(UDF_GROUP, 8),
    GroupInfo(VISUALIZATION_GROUP, 9)
  )

}
