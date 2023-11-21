package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeTypeUtils, Schema}

import java.io._
import java.nio.file.{Files, Paths}
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters.{
  asScalaIteratorConverter,
  enumerationAsScalaIteratorConverter
}

class TextScanSourceOpExec private[text] (
    val desc: TextScanSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAttributeName: String
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var lineReader: BufferedReader = _
  private var zipReader: ZipFile = _

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType.isOutputSingleTuple) {
      if (0x504b0304 == new RandomAccessFile(desc.filePath.get, "r").readInt()) {
        zipReader = new ZipFile(desc.filePath.get)
        zipReader
          .entries()
          .asScala
          .map(entry => singleTuple(zipReader.getInputStream(entry).readAllBytes))
      } else {
        Iterator(singleTuple(Files.readAllBytes(Paths.get(desc.filePath.get))))
      }
    } else { // normal text file scan mode
      if (0x504b0304 == new RandomAccessFile(desc.filePath.get, "r").readInt()) {
        zipReader = new ZipFile(desc.filePath.get)
        zipReader
          .entries()
          .asScala
          .flatMap(entry => {
            lineReader = new BufferedReader(
              new InputStreamReader(
                zipReader.getInputStream(entry),
                desc.fileEncodingHideable.getCharset
              )
            )
            multipleTuple
          })
      } else {
        lineReader = new BufferedReader(
          new FileReader(desc.filePath.get, desc.fileEncodingHideable.getCharset)
        )
        multipleTuple
      }
    }
  }

  private def singleTuple(file: Array[Byte]): Tuple =
    Tuple
      .newBuilder(schema)
      .add(
        schema.getAttribute(outputAttributeName),
        desc.attributeType match {
          case TextScanSourceAttributeType.BINARY => file
          case TextScanSourceAttributeType.STRING_AS_SINGLE_TUPLE =>
            new String(file, desc.fileEncodingHideable.getCharset)
        }
      )
      .build()

  private def multipleTuple: Iterator[Tuple] = {
    lineReader
      .lines()
      .iterator()
      .asScala
      .slice(startOffset, endOffset)
      .map(line => {
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute(outputAttributeName),
            AttributeTypeUtils.parseField(line.asInstanceOf[Object], desc.attributeType.getType)
          )
          .build()
      })
  }

  override def open(): Unit =
    schema = desc.sourceSchema()

  override def close(): Unit = {
    if (lineReader != null && zipReader == null) lineReader.close()
    else if (zipReader != null && lineReader == null) zipReader.close()
  }
}
