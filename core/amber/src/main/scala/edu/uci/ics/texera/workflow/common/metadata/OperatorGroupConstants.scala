package edu.uci.ics.texera.workflow.common.metadata

object OperatorGroupConstants {
  final val SOURCE_GROUP = "Source"
  final val SEARCH_GROUP = "Search"
  final val ANALYTICS_GROUP = "Analytics"
  final val SPLIT_GROUP = "Split"
  final val JOIN_GROUP = "Join"
  final val UTILITY_GROUP = "Utilities"
  final val DATABASE_GROUP = "Database"
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
    GroupInfo(SPLIT_GROUP, 3),
    GroupInfo(JOIN_GROUP, 4),
    GroupInfo(UTILITY_GROUP, 5),
    GroupInfo(DATABASE_GROUP, 6),
    GroupInfo(UDF_GROUP, 7),
    GroupInfo(VISUALIZATION_GROUP, 8),
    GroupInfo(RESULT_GROUP, 9)
  )

}
