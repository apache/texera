package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.{ExecutionStatusEnum, Running}

object JsonTest {

  def main(args: Array[String]): Unit = {
    val a = Running
    val om = Utils.objectMapper

    val str = om.writeValueAsString(a)
    println(str)

    val des = om.readValue(str, classOf[ExecutionStatusEnum])
    println(des)

  }
}

class JsonTest {}
