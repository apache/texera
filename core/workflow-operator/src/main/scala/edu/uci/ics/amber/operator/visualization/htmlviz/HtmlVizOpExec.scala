package edu.uci.ics.amber.operator.visualization.htmlviz

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(htmlContentAttrName: String) extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    Iterator(TupleLike(tuple.getField[Any](htmlContentAttrName)))
}
