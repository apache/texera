package edu.uci.ics.texera.workflow.operators.visualization.urlviz

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * URL Visualization operator to render any given URL link
  */
class UrlVizOpExec(urlContentAttrName: String) extends OperatorExecutor {

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val iframe =
      s"""<!DOCTYPE html>
         |<html lang="en">
         |<body>
         |  <div class="modal-body">
         |    <iframe src="${tuple.getField(urlContentAttrName)}" frameborder="0"
         |       style="height:100vh; width:100%; border:none;">
         |    </iframe>
         |  </div>
         |</body>
         |</html>""".stripMargin
    Iterator(TupleLike(iframe))
  }
}
