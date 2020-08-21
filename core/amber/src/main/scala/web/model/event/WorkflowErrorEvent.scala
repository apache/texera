package web.model.event

import texera.common.TexeraConstraintViolation

import scala.beans.BeanProperty
import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable

case class WorkflowErrorEvent
(
  operatorErrors: Map[String, Set[TexeraConstraintViolation]] =
    new HashMap[String, Set[TexeraConstraintViolation]](),
  generalErrors: Map[String, String] = new HashMap[String, String]()
) extends TexeraWsEvent
