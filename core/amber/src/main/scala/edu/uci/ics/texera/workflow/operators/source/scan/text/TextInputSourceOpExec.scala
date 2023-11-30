package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.operators.source.scan.FileAttributeType

class TextInputSourceOpExec private[text] (val desc: TextInputSourceOpDesc)
    extends SourceOperatorExecutor {

  override def produceTexeraTuple(): Iterator[Tuple] = {
    (if (desc.attributeType.isSingle) {
       Iterator(desc.textInput)
     } else {
       desc.textInput.linesIterator.slice(
         desc.fileScanOffset.getOrElse(0),
         desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
       )
     }).map(line =>
      Tuple
        .newBuilder(desc.sourceSchema())
        .add(
          desc.sourceSchema().getAttributes.get(0),
          desc.attributeType match {
            case FileAttributeType.SINGLE_STRING => line
            case FileAttributeType.BINARY        => line.getBytes
            case _                               => parseField(line, desc.attributeType.getType)
          }
        )
        .build()
    )
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
