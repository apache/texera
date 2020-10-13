package edu.uci.ics.texera.web

import edu.uci.ics.texera.workflow.common.TexeraUtils
import edu.uci.ics.texera.workflow.common.workflow.{
  TexeraBreakpoint,
  TexeraBreakpointCondition,
  TexeraConditionBreakpoint
}
import edu.uci.ics.texera.web.model.request.{ExecuteWorkflowRequest, HelloWorldRequest}

object JsonTest {

  def main(args: Array[String]): Unit = {
    val a = TexeraConditionBreakpoint("0", TexeraBreakpointCondition.EQ, "100")
    val om = TexeraUtils.objectMapper

    val str = om.writeValueAsString(a)
    println(str)

    val des = om.readValue(str, classOf[TexeraBreakpoint])
    println(des)

  }
}

class JsonTest {}
