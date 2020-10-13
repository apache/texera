package texera.common.workflow

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
    new Type(value = classOf[TexeraConditionBreakpoint], name = "ConditionBreakpoint"),
    new Type(value = classOf[TexeraCountBreakpoint], name = "CountBreakpoint")
  )
)
trait TexeraBreakpoint {}

object TexeraBreakpointCondition extends Enumeration {
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

class ConditionType extends TypeReference[TexeraBreakpointCondition.type]

case class TexeraConditionBreakpoint(
    column: String,
    @JsonScalaEnumeration(classOf[ConditionType]) condition: TexeraBreakpointCondition.Condition,
    value: String
) extends TexeraBreakpoint

case class TexeraCountBreakpoint(count: Long) extends TexeraBreakpoint
