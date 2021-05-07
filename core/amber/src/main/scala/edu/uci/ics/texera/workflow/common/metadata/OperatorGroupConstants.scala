package edu.uci.ics.texera.workflow.common.metadata

object OperatorGroupConstants {
  final val SOURCE_GROUP = "Source"
  final val SEARCH_GROUP = "Search"
  final val ANALYTICS_GROUP = "Analytics"
  final val JOIN_GROUP = "Join"
  final val UTILITY_GROUP = "Utilities"
  final val UDF_GROUP = "User-defined Functions"
  final val VISUALIZATION_GROUP = "Visualization"
  final val RESULT_GROUP = "View Results"

  /**
    * The order of the groups to show up in the frontend operator panel.
    * The order numbers are relative.
    */
  final val OperatorGroupOrderList: List[GroupInfo] = List(
    GroupInfo(SOURCE_GROUP, 0),
    GroupInfo(SEARCH_GROUP, 1),
    GroupInfo(ANALYTICS_GROUP, 2),
    GroupInfo(JOIN_GROUP, 3),
    GroupInfo(UTILITY_GROUP, 4),
    GroupInfo(UDF_GROUP, 5),
    GroupInfo(VISUALIZATION_GROUP, 6),
    GroupInfo(RESULT_GROUP, 7)
  )

}
