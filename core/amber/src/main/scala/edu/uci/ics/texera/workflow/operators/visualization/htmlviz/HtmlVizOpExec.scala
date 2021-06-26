package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import scala.collection.Iterator
import scala.util.Either

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(htmlContentAttrName: String, operatorSchemaInfo: OperatorSchemaInfo)
    extends OperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] =
    tuple match {
      case Left(t) =>
        val result = Tuple
          .newBuilder(operatorSchemaInfo.outputSchema)
          .add("html-content", AttributeType.STRING, t.getField(htmlContentAttrName))
          .build()
        Iterator(result)

      case Right(_) => Iterator()
    }
}
