package edu.uci.ics.amber.operator.visualization.urlviz

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

/**
  * URL Visualization operator to render any given URL link
  */
class UrlVizOpExec(descString: String) extends OperatorExecutor {
  private val desc: UrlVizOpDesc = objectMapper.readValue(descString, classOf[UrlVizOpDesc])
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val iframe =
      s"""<!DOCTYPE html>
         |<html lang="en">
         |<body>
         |  <div class="modal-body">
         |    <iframe src="${tuple.getField(desc.urlContentAttrName)}" frameborder="0"
         |       style="height:100vh; width:100%; border:none;">
         |    </iframe>
         |  </div>
         |</body>
         |</html>""".stripMargin
    Iterator(TupleLike(iframe))
  }
}
