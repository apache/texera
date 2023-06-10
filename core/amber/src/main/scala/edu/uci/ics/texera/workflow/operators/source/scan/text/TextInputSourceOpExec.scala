package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeTypeUtils, Schema}

class TextInputSourceOpExec private[text] (
    val desc: TextInputSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAttributeName: String
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var rows: Iterator[String] = _

  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType.isOutputSingleTuple) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute(outputAttributeName),
            desc.attributeType match {
              // AttributeTypeUtils doesn't parse String into Bytes[], do it ourselves
              case TextSourceAttributeType.BINARY =>
                desc.textInput.getBytes(desc.fileEncoding.getCharset)
              case TextSourceAttributeType.STRING_AS_SINGLE_TUPLE => desc.textInput
            }
          )
          .build()
      )
    } else {
      rows.map(line => {
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute(outputAttributeName),
            AttributeTypeUtils.parseField(line.asInstanceOf[Object], desc.attributeType.getType)
          )
          .build()
      })
    }
  }

  override def open(): Unit = {
    schema = desc.sourceSchema()
    if (!desc.attributeType.isOutputSingleTuple) {
      rows = desc.textInput.linesIterator.slice(startOffset, endOffset)
    }
  }

  override def close(): Unit = {}
}
