package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class TextInputSourceOpExec private[text] (
    val desc: TextInputSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAttributeName: String
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var rows: Iterator[String] = _

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.outputAsSingleTuple) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(schema.getAttribute(outputAttributeName), desc.textInput)
          .build()
      )
    } else {
      rows.map(line => {
        Tuple.newBuilder(schema).add(schema.getAttribute(outputAttributeName), line).build()
      })
    }
  }

  override def open(): Unit = {
    schema = desc.sourceSchema()
    if (!desc.outputAsSingleTuple)
      rows = desc.textInput.linesIterator.slice(startOffset, endOffset)
  }

  override def close(): Unit = {}
}
