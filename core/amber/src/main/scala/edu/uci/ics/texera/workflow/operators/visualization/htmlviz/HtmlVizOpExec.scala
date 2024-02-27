package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(htmlContentAttrName: String, outputSchema: Schema) extends OperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] =
    tuple match {
      case Left(t) =>
        val result = Tuple
          .newBuilder(outputSchema)
          .add("html-content", AttributeType.STRING, t.getField(htmlContentAttrName))
          .build()
        Iterator(result)

      case Right(_) => Iterator()
    }
}
