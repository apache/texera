package edu.uci.ics.amber.operator.source.scan.text

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.scan.FileAttributeType
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class TextInputSourceOpExec private[text] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: TextInputSourceOpDesc =
    objectMapper.readValue(descString, classOf[TextInputSourceOpDesc])
  override def produceTuple(): Iterator[TupleLike] = {
    (if (desc.attributeType.isSingle) {
       Iterator(desc.textInput)
     } else {
       desc.textInput.linesIterator.slice(
         desc.fileScanOffset.getOrElse(0),
         desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
       )
     }).map(line =>
      TupleLike(desc.attributeType match {
        case FileAttributeType.SINGLE_STRING => line
        case FileAttributeType.BINARY        => line.getBytes
        case _                               => parseField(line, desc.attributeType.getType)
      })
    )
  }

}
