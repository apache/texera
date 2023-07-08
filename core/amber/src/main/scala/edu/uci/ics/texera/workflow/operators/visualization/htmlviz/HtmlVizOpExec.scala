package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import scala.collection.Iterator
import scala.util.Either

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(
    htmlContentAttrName: String,
    url: Boolean,
    operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] =
    tuple match {
      case Left(t) =>
        val result = if (url) {
          val iframe =
            "<!DOCTYPE html>\n<html lang=\"en\"><body><div class=\"modal-body\">\n<iframe src=\"" + t
              .getField(
                htmlContentAttrName
              ) + "\" width=\"100%\" height=\"500px\"></iframe>\n</div></body>\n</html>"
          Tuple
            .newBuilder(operatorSchemaInfo.outputSchemas(0))
            .add("html-content", AttributeType.STRING, iframe)
            .build()
        } else {
          Tuple
            .newBuilder(operatorSchemaInfo.outputSchemas(0))
            .add("html-content", AttributeType.STRING, t.getField(htmlContentAttrName))
            .build()
        }
        Iterator(result)

      case Right(_) => Iterator()
    }
}
