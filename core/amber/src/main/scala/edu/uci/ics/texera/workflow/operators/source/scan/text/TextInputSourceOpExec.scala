package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.operators.source.scan.FileAttributeType

class TextInputSourceOpExec private[text] (
    fileAttributeType: FileAttributeType,
    textInput: String,
    fileScanOffset: Option[Int] = None,
    fileScanLimit: Option[Int] = None
) extends SourceOperatorExecutor {

  override def produceTuple(): Iterator[TupleLike] = {
    (if (fileAttributeType.isSingle) {
       Iterator(textInput)
     } else {
       textInput.linesIterator.slice(
         fileScanOffset.getOrElse(0),
         fileScanOffset.getOrElse(0) + fileScanLimit.getOrElse(Int.MaxValue)
       )
     }).map(line =>
      TupleLike(fileAttributeType match {
        case FileAttributeType.SINGLE_STRING => line
        case FileAttributeType.BINARY        => line.getBytes
        case _                               => parseField(line, fileAttributeType.getType)
      })
    )
  }

}
