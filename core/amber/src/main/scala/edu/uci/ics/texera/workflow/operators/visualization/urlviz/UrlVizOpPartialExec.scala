package edu.uci.ics.texera.workflow.operators.visualization.urlviz

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

/**
  * URL Visualization operator to render any given URL link
  */
class UrlVizOpPartialExec(
    htmlContentAttrName: String,
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
        val iframe =
          "<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "<body><div class=\"modal-body\">\n" + "<iframe src=\"" + t
            .getField(
              htmlContentAttrName
            ) + "\" frameborder=\"0\" style=\"height:100vh; width:100%; border:none;\"></iframe>\n" + "</div></body>\n" + "</html>"
        Iterator(
          Tuple
            .newBuilder(operatorSchemaInfo.outputSchemas(0))
            .add("html-content", AttributeType.STRING, iframe)
            .build()
        )

      case Right(_) => Iterator()
    }
}
