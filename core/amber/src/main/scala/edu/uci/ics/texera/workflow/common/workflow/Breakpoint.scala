package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[ConditionBreakpoint], name = "ConditionBreakpoint"),
    new Type(value = classOf[CountBreakpoint], name = "CountBreakpoint")
  )
)
trait Breakpoint {}

object BreakpointCondition extends Enumeration {
  type Condition = Value
  val EQ: Condition = Value("=")
  val LT: Condition = Value("<")
  val LE: Condition = Value("<=")
  val GT: Condition = Value(">")
  val GE: Condition = Value(">=")
  val NE: Condition = Value("!=")
  val CONTAINS: Condition = Value("contains")
  val NOT_CONTAINS: Condition = Value("does not contain")
}

class ConditionType extends TypeReference[BreakpointCondition.type]

case class ConditionBreakpoint(
    column: String,
    @JsonScalaEnumeration(classOf[ConditionType]) condition: BreakpointCondition.Condition,
    value: String
) extends Breakpoint

case class CountBreakpoint(count: Long) extends Breakpoint
