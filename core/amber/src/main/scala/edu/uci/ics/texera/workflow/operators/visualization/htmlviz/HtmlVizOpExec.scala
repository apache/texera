package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(htmlContentAttrName: String) extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    Iterator(TupleLike(tuple.getField[Any](htmlContentAttrName)))
}
