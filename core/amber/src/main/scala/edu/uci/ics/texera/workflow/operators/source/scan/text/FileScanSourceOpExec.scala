package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils

import java.io._
import java.nio.file.{Files, Paths}
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters.{
  asScalaIteratorConverter,
  enumerationAsScalaIteratorConverter
}

class FileScanSourceOpExec private[text] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.unzip) {
      val zipReader = new ZipFile(desc.filePath.get)
      val entries =
        zipReader.entries().asScala.filterNot(entry => entry.getName.startsWith("__MACOSX"))
      if (desc.attributeType.isSingle) {
        entries.map(entry => singleTuple(zipReader.getInputStream(entry).readAllBytes))
      } else {
        entries.flatMap(entry =>
          multipleTuple(
            new InputStreamReader(zipReader.getInputStream(entry), desc.encoding.getCharset)
          )
        )
      }
    } else {
      if (desc.attributeType.isSingle) {
        Iterator(singleTuple(Files.readAllBytes(Paths.get(desc.filePath.get))))
      } else {
        multipleTuple(new FileReader(desc.filePath.get, desc.encoding.getCharset))
      }
    }
  }

  private def singleTuple(file: Array[Byte]): Tuple =
    new Tuple(
      desc.sourceSchema(),
      desc.attributeType match {
        case FileAttributeType.BINARY        => file
        case FileAttributeType.SINGLE_STRING => new String(file, desc.encoding.getCharset)
      }
    )

  private def multipleTuple(reader: Reader): Iterator[Tuple] = {
    new BufferedReader(reader)
      .lines()
      .iterator()
      .asScala.slice(desc.fileScanOffset.getOrElse(0), desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue))
      .map(line =>
        new Tuple(
          desc.sourceSchema(),
          AttributeTypeUtils.parseField(line, desc.attributeType.getType)
        )
      )
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
