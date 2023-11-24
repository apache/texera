package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils

class TextInputSourceOpExec private[text] (val desc: TextInputSourceOpDesc)
    extends SourceOperatorExecutor {

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType.isSingle) {
      Iterator(
        new Tuple(
          desc.sourceSchema(),
          desc.attributeType match {
            case FileAttributeType.BINARY        => desc.textInput.getBytes()
            case FileAttributeType.SINGLE_STRING => desc.textInput
          }
        )
      )
    } else {
      desc.textInput.linesIterator
        .slice(
          desc.fileScanOffset.getOrElse(0),
          desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
        )
        .map(line =>
          new Tuple(
            desc.sourceSchema(),
            AttributeTypeUtils.parseField(line, desc.attributeType.getType)
          )
        )
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
