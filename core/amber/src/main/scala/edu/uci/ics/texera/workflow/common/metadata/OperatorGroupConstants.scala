package edu.uci.ics.texera.workflow.common.metadata

object OperatorGroupConstants {
  final val SOURCE_GROUP = "Source"
  final val API_GROUP = "API"
  final val SEARCH_GROUP = "Search"
  final val MACHINE_LEARNING_GROUP = "Machine Learning"
  final val JOIN_GROUP = "Join"
  final val UTILITY_GROUP = "Utilities"
  final val SET_GROUP = "Set"
  final val UDF_GROUP = "User-defined Functions"
  final val VISUALIZATION_GROUP = "Visualization"

  /**
    * The order of the groups to show up in the frontend operator panel.
    * The order numbers are relative.
    */
  final val OperatorGroupOrderList: List[GroupInfo] = List(
    GroupInfo(SOURCE_GROUP, 0),
    GroupInfo(API_GROUP, 1),
    GroupInfo(SEARCH_GROUP, 2),
    GroupInfo(MACHINE_LEARNING_GROUP, 3),
    GroupInfo(JOIN_GROUP, 4),
    GroupInfo(SET_GROUP, 5),
    GroupInfo(UTILITY_GROUP, 6),
    GroupInfo(UDF_GROUP, 7),
    GroupInfo(VISUALIZATION_GROUP, 8)
  )

}
