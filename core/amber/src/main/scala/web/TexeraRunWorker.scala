package web

import Engine.Common.AmberUtils
import texera.common.TexeraUtils

object TexeraRunWorker {

  def main(args: Array[String]): Unit = {
    // start actor system worker node
    AmberUtils.startActorWorker(Option.empty)
  }

}
