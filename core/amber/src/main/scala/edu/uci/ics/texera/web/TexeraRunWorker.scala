package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.common.AmberUtils

object TexeraRunWorker {

  def main(args: Array[String]): Unit = {
    // start actor system worker node
    if (args != null && args.length > 0) {
      AmberUtils.startActorWorker(Option.apply(args(0)))
    } else {
      AmberUtils.startActorWorker(Option.empty)
    }
  }

}
