package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.RUNNING
import edu.uci.ics.texera.Utils

object JsonTest {

  def main(args: Array[String]): Unit = {
    val a = RUNNING
    val om = Utils.objectMapper

    val str = om.writeValueAsString(a)
    println(str)

    val des = om.readValue(str, classOf[WorkflowAggregatedState])
    println(des)

  }
}

class JsonTest {}
