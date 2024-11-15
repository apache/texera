package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.amber.engine.common.executor.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.TupleLike
import edu.uci.ics.amber.engine.common.model.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.operators.source.scan.FileAttributeType

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
