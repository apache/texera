package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.workflow.common.TexeraUtils

object TexeraRunWorker {

  def main(args: Array[String]): Unit = {
    // start actor system worker node
    AmberUtils.startActorWorker(Option.empty)
  }

}
