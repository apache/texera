package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.common.workflow.{
  Breakpoint,
  BreakpointCondition,
  ConditionBreakpoint
}

object JsonTest {

  def main(args: Array[String]): Unit = {
    val a = ConditionBreakpoint("0", BreakpointCondition.EQ, "100")
    val om = Utils.objectMapper

    val str = om.writeValueAsString(a)
    println(str)

    val des = om.readValue(str, classOf[Breakpoint])
    println(des)

  }
}

class JsonTest {}
