package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, AttributeTypeUtils, Schema}

class TextInputSourceOpExec private[text] (val desc: TextInputSourceOpDesc) extends SourceOperatorExecutor {
  private val schema: Schema = desc.sourceSchema()

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType == AttributeType.STRING) {
      Iterator(new Tuple(schema, desc.textInput))
    } else {
      desc.textInput.linesIterator
        .drop(desc.fileScanOffset.getOrElse(0))
        .take(desc.fileScanLimit.getOrElse(Int.MaxValue))
        .map(line => new Tuple(schema, AttributeTypeUtils.parseField(line, desc.attributeType)))
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
