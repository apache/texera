package texera.common.schema

import java.util

object OperatorGroupConstants {
  final val SOURCE_GROUP = "Source"
  final val SEARCH_GROUP = "Search"
  final val ANALYTICS_GROUP = "Analytics"
  final val SPLIT_GROUP = "Split"
  final val JOIN_GROUP = "Join"
  final val UTILITY_GROUP = "Utilities"
  final val DATABASE_GROUP = "Database"
  final val RESULT_GROUP = "View Results"

  /**
    * The order of the groups to show up in the frontend operator panel.
    * The order numbers are relative.
    */
  final val OperatorGroupOrderList = new util.ArrayList[GroupOrder]
  OperatorGroupOrderList.add(GroupOrder(SOURCE_GROUP, 0))
  OperatorGroupOrderList.add(GroupOrder(SEARCH_GROUP, 1))
  OperatorGroupOrderList.add(GroupOrder(ANALYTICS_GROUP, 2))
  OperatorGroupOrderList.add(GroupOrder(SPLIT_GROUP, 3))
  OperatorGroupOrderList.add(GroupOrder(JOIN_GROUP, 4))
  OperatorGroupOrderList.add(GroupOrder(UTILITY_GROUP, 5))
  OperatorGroupOrderList.add(GroupOrder(DATABASE_GROUP, 6))
  OperatorGroupOrderList.add(GroupOrder(RESULT_GROUP, 7))
}
