package edu.uci.ics.amber.operator.visualization.htmlviz

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(descString: String) extends OperatorExecutor {
  private val desc: HtmlVizOpDesc = objectMapper.readValue(descString, classOf[HtmlVizOpDesc])
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    Iterator(TupleLike(tuple.getField[Any](desc.htmlContentAttrName)))
}
