package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils

class TextInputSourceOpExec private[text] (val desc: TextInputSourceOpDesc) extends SourceOperatorExecutor {

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType == FileAttributeType.SINGLE_STRING) {
      Iterator(new Tuple(desc.sourceSchema(), desc.textInput))
    } else if (desc.attributeType == FileAttributeType.BINARY) {
      Iterator(new Tuple(desc.sourceSchema(), desc.textInput.getBytes()))
    } else {
      desc.textInput.linesIterator
        .drop(desc.fileScanOffset.getOrElse(0))
        .take(desc.fileScanLimit.getOrElse(Int.MaxValue))
        .map(line => new Tuple(desc.sourceSchema(), AttributeTypeUtils.parseField(line, desc.attributeType.getType)))
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
