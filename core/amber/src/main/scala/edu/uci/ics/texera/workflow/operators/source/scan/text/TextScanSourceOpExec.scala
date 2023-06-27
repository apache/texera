package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeTypeUtils, Schema}

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class TextScanSourceOpExec private[text] (
    val desc: TextScanSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAttributeName: String
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var reader: BufferedReader = _
  private var rows: Iterator[String] = _
  private var path: Path = _

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType.isOutputSingleTuple) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute(outputAttributeName),
            desc.attributeType match {
              case TextScanSourceAttributeType.STRING_AS_SINGLE_TUPLE =>
                new String(Files.readAllBytes(path), desc.fileEncodingHideable.getCharset)
              case TextScanSourceAttributeType.BINARY => Files.readAllBytes(path)
            }
          )
          .build()
      )
    } else { // normal text file scan mode
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
    schema = desc.inferSchema()
    if (desc.attributeType.isOutputSingleTuple) {
      path = Paths.get(desc.filePath.get)
    } else {
      reader = new BufferedReader(
        new InputStreamReader(
          new FileInputStream(desc.filePath.get),
          desc.fileEncodingHideable.getCharset
        )
      )
      rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
    }
  }

  // in outputAsSingleTuple mode, Files.readAllBytes handles the closing of file
  override def close(): Unit = if (!desc.attributeType.isOutputSingleTuple) reader.close()
}
