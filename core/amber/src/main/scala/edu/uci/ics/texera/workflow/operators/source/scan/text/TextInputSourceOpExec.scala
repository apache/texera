package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, AttributeTypeUtils, Schema}

class TextInputSourceOpExec private[text] (val desc: TextInputSourceOpDesc) extends SourceOperatorExecutor {
  private val schema: Schema = desc.sourceSchema()

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType == AttributeType.STRING) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttributes.get(0),
            desc.textInput
          )
          .build()
      )
    } else {
      desc.textInput.linesIterator
        .drop(desc.fileScanOffset.getOrElse(0))
        .take(desc.fileScanLimit.getOrElse(Int.MaxValue))
        .map(line => {
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttributes.get(0),
            AttributeTypeUtils.parseField(line.asInstanceOf[Object], desc.attributeType)
          )
          .build()
      })
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
