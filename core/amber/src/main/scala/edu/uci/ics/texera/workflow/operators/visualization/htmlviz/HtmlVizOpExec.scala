package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

/**
 * HTML Visualization operator to render any given HTML code
 */
class HtmlVizOpExec(htmlContentAttrName: String) extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    Iterator(TupleLike(tuple.getField[Any](htmlContentAttrName)))
}
