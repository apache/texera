package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeTypeUtils, Schema}

import java.io._
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipFile, ZipInputStream}
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, enumerationAsScalaIteratorConverter}

class FileScanSourceOpExec private[text] (val desc: FileScanSourceOpDesc) extends SourceOperatorExecutor {
  private val schema: Schema = desc.sourceSchema()

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.attributeType == FileAttributeType.SINGLE_STRING || desc.attributeType == FileAttributeType.BINARY) {
      if (desc.unzip) {
        val zipReader = new ZipFile(desc.filePath.get)
        new ZipInputStream(new FileInputStream(desc.filePath.get))
        zipReader
          .entries()
          .asScala
          .map(entry => singleTuple(zipReader.getInputStream(entry).readAllBytes))
      } else {
        Iterator(singleTuple(Files.readAllBytes(Paths.get(desc.filePath.get))))
      }
    } else {
      if (desc.unzip) {
        val zipReader = new ZipFile(desc.filePath.get)
        zipReader
          .entries()
          .asScala
          .flatMap(entry => multipleTuple(new InputStreamReader(zipReader.getInputStream(entry), desc.encoding.getCharset))
          )
      } else {
        multipleTuple(new FileReader(desc.filePath.get, desc.encoding.getCharset))
      }
    }
  }

  private def singleTuple(file: Array[Byte]): Tuple =
    new Tuple(schema, desc.attributeType match {
          case FileAttributeType.BINARY => file
          case FileAttributeType.SINGLE_STRING =>
            new String(file, desc.encoding.getCharset)
        })

  private def multipleTuple(reader: Reader): Iterator[Tuple] = {
    new BufferedReader(reader)
      .lines()
      .iterator()
      .asScala
      .drop(desc.fileScanOffset.getOrElse(0))
      .take(desc.fileScanLimit.getOrElse(Int.MaxValue))
      .map(line => new Tuple(schema, AttributeTypeUtils.parseField(line, desc.attributeType.getType)))
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
