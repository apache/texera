package edu.uci.ics.amber.operator.source.scan.text

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.scan.FileAttributeType

class TextInputSourceOpExec private[text] (
    fileAttributeType: FileAttributeType,
    textInput: String,
    fileScanLimit: Option[Int] = None,
    fileScanOffset: Option[Int] = None
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
