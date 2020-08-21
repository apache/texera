package texera.common.schema

import scala.beans.BeanProperty


case class GroupOrder
(@BeanProperty groupName: String, @BeanProperty groupOrder: Int)
